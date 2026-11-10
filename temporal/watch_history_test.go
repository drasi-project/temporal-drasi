//go:build integration && temporal_with_drasi && cgo

package temporal_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/sdk/go/condition"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	watchservice "go.temporal.io/server/service/watch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type watchHistoryPipeline struct {
	client        sdkclient.Client
	control       controlpb.ControlServiceClient
	definitions   contractpb.WatchServiceClient
	registry      registrypb.RegistryServiceClient
	query         *contractpb.Query
	watch         *contractpb.Watch
	namespaceID   string
	controlID     string
	producerType  string
	taskQueue     string
	consumerID    string
	conditionType string
	helperType    string
	helperClient  *conditionResponseLossClient
}

func verifyWatchHistoryPipeline(t *testing.T, frontend, history *grpc.ClientConn, source *contractpb.Source, namespaceID string) *watchHistoryPipeline {
	t.Helper()
	ctx := testcontext.For(t)
	tv := testvars.New(t)
	client, err := sdkclient.Dial(sdkclient.Options{HostPort: frontend.Target(), Namespace: source.Namespace})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	worker := sdkworker.New(client, tv.TaskQueue().Name, sdkworker.Options{})
	definitions := contractpb.NewWatchServiceClient(frontend)
	helperClient := &conditionResponseLossClient{WatchServiceClient: definitions, lost: make(chan *contractpb.CreateWatchRequest, 1)}
	require.NoError(t, condition.RegisterActivities(worker, helperClient))
	helperType := tv.WorkflowType().Name + "ConditionHelper"
	worker.RegisterWorkflowWithOptions(watchConditionHelperWorkflow, workflow.RegisterOptions{Name: helperType})
	producerType, consumerType := tv.WorkflowType().Name+"Producer", tv.WorkflowType().Name+"Consumer"
	worker.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		var done bool
		workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &done)
		return nil
	}, workflow.RegisterOptions{Name: producerType})
	worker.RegisterWorkflowWithOptions(func(_ workflow.Context, result json.RawMessage) (string, error) {
		return workflowIDFromWatchResult(result)
	}, workflow.RegisterOptions{Name: consumerType})
	conditionType := tv.WorkflowType().Name + "Condition"
	worker.RegisterWorkflowWithOptions(func(ctx workflow.Context) (string, error) {
		var result json.RawMessage
		workflow.GetSignalChannel(ctx, "condition").Receive(ctx, &result)
		return workflowIDFromWatchResult(result)
	}, workflow.RegisterOptions{Name: conditionType})
	require.NoError(t, worker.Start())
	t.Cleanup(worker.Stop)
	producer, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: tv.WorkflowID(), TaskQueue: tv.TaskQueue().Name,
	}, producerType)
	require.NoError(t, err)
	query, err := definitions.CreateQuery(ctx, &contractpb.CreateQueryRequest{
		Namespace: source.Namespace, Id: "runtime.completed", RequestId: tv.Any().String(),
		Spec: &contractpb.QuerySpec{
			Language: "cypher", StateStore: "persistent",
			Text: fmt.Sprintf("MATCH (w:Workflow) WHERE w.data.type = '%s' AND w.data.status = '%s' RETURN w.workflow_id AS workflow_id",
				producerType, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED.String()),
			Sources: []*contractpb.ResourceRef{{Id: source.Id, Revision: source.Revision}},
		},
	})
	require.NoError(t, err)
	query, err = definitions.StartQuery(ctx, &contractpb.QueryOperationRequest{
		Namespace: query.Namespace, Id: query.Id, ExpectedRevision: query.Revision, RequestId: tv.Any().String(),
	})
	require.NoError(t, err)
	actionWorkflowID := tv.WithWorkflowIDNumber(2).WorkflowID()
	resource, err := definitions.CreateWatch(ctx, &contractpb.CreateWatchRequest{
		Namespace: source.Namespace, Id: "runtime.completed.watch", RequestId: tv.Any().String(),
		Spec: &contractpb.WatchSpec{
			Query:    &contractpb.ResourceRef{Id: query.Id, Revision: query.Revision},
			Trigger:  contractpb.Trigger_TRIGGER_ENTER,
			Lifetime: &contractpb.WatchSpec_Persistent{Persistent: &emptypb.Empty{}},
			Action: &contractpb.WorkflowAction{Operation: &contractpb.WorkflowAction_Start{Start: &contractpb.WorkflowStart{
				WorkflowId: actionWorkflowID, WorkflowType: consumerType, TaskQueue: tv.TaskQueue().Name,
				ReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			}}},
		},
	})
	require.NoError(t, err)
	control := controlpb.NewControlServiceClient(history)
	pipeline := &watchHistoryPipeline{
		client: client, control: control, definitions: definitions,
		registry: registrypb.NewRegistryServiceClient(history), query: query, watch: resource, namespaceID: namespaceID,
		producerType: producerType, taskQueue: tv.TaskQueue().Name,
		consumerID: actionWorkflowID, conditionType: conditionType, helperType: helperType, helperClient: helperClient,
	}
	id := pipeline.executionID(t, resource)
	pipeline.controlID = id
	await.Require(ctx, t, func(t *await.T) {
		response, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{NamespaceId: namespaceID, ExecutionId: id})
		require.NoError(t, err)
		require.Equal(t, controlpb.STATUS_RUNNING, response.State.Status)
		require.Equal(t, controlpb.PROGRESS_PHASE_LIVE, response.State.ProgressPhase)
		require.Zero(t, response.State.Delivered)
	}, 30*time.Second, 100*time.Millisecond)
	observed, err := definitions.GetWatch(ctx, &contractpb.GetWatchRequest{
		Namespace: resource.Namespace, Id: resource.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, observed.Status, "a live Watch must expose its observed runtime status")
	pipeline.requireRunningResources(t, source)
	pipeline.verifyQueryValidation(t, source)
	require.NoError(t, client.SignalWorkflow(ctx, producer.GetID(), producer.GetRunID(), "finish", true))
	require.NoError(t, producer.Get(ctx, nil))
	pipeline.requireDelivery(t, 1, producer.GetID())
	pipeline.verifyConditions(t, producer.GetID())
	pipeline.verifyConditionHelper(t, producer.GetID())
	return pipeline
}

func (pipeline *watchHistoryPipeline) verifyQueryValidation(t *testing.T, source *contractpb.Source) {
	t.Helper()
	before, err := watchservice.ReadDesiredView(t.Context(), pipeline.registry, pipeline.namespaceID, source.Namespace)
	require.NoError(t, err)
	capabilities, err := pipeline.definitions.GetCapabilities(t.Context(), &contractpb.GetCapabilitiesRequest{Namespace: source.Namespace})
	require.NoError(t, err)
	require.Equal(t, []string{"cypher", "gql"}, capabilities.QueryLanguages)
	require.Equal(t, []string{"persistent"}, capabilities.StateStores)
	require.Contains(t, capabilities.AllowedMethods, contractpb.WatchService_ValidateQuery_FullMethodName)
	require.NotContains(t, capabilities.AllowedMethods, contractpb.WatchService_PreviewQuery_FullMethodName)
	require.Zero(t, capabilities.MaxPreviewRows)
	for _, language := range capabilities.QueryLanguages {
		for _, tc := range []struct {
			text string
			want *contractpb.ValidateQueryResponse
		}{
			{"MATCH (w:Workflow) RETURN w.workflow_id AS workflow_id", &contractpb.ValidateQueryResponse{Valid: true}},
			{"MATCH (w:Workflow) RETURN count(w) AS total", &contractpb.ValidateQueryResponse{Valid: true}},
			{"MATCH (", &contractpb.ValidateQueryResponse{Diagnostics: []*contractpb.Diagnostic{{
				Field: "spec.text", Message: "Query syntax or structure is invalid",
			}}}},
		} {
			spec := common.CloneProto(pipeline.query.Spec)
			spec.Language, spec.Text = language, tc.text
			response, err := pipeline.definitions.ValidateQuery(t.Context(), &contractpb.ValidateQueryRequest{
				Namespace: source.Namespace, Spec: spec,
			})
			require.NoError(t, err, "query validation must reach the native engine without creating a query")
			protorequire.ProtoEqual(t, tc.want, response)
		}
	}
	after, err := watchservice.ReadDesiredView(t.Context(), pipeline.registry, pipeline.namespaceID, source.Namespace)
	require.NoError(t, err)
	require.Equal(t, before.Generation(), after.Generation(), "compilation must not register or change a resource")
}

func (pipeline *watchHistoryPipeline) verifyExternalQueryValidation(t *testing.T, source *contractpb.Source) {
	t.Helper()
	tv := testvars.New(t)
	external, err := pipeline.definitions.CreateSource(t.Context(), &contractpb.CreateSourceRequest{
		Namespace: source.Namespace, Id: "runtime.schema-only", RequestId: tv.Any().String(),
		Spec: &contractpb.SourceSpec{
			Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
				Plugin: &contractpb.PluginComponentRef{
					Digest: "sha256:" + strings.Repeat("c", 64), Component: "unloaded-source",
				},
				Secrets: map[string]*contractpb.SecretReference{
					"connection": {Provider: "unconfigured-provider", Key: "configuration", Version: "v1"},
				},
			}},
			Bootstrap: &contractpb.ComponentConfiguration{Plugin: &contractpb.PluginComponentRef{
				Digest: "sha256:" + strings.Repeat("d", 64), Component: "unloaded-bootstrap",
			}},
		},
	})
	require.NoError(t, err)
	before, err := watchservice.ReadDesiredView(t.Context(), pipeline.registry, pipeline.namespaceID, source.Namespace)
	require.NoError(t, err)
	spec := common.CloneProto(pipeline.query.Spec)
	spec.Sources = append(spec.Sources, &contractpb.ResourceRef{Id: external.Id, Revision: external.Revision})
	response, err := pipeline.definitions.ValidateQuery(t.Context(), &contractpb.ValidateQueryRequest{
		Namespace: source.Namespace, Spec: spec,
	})
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code(),
		"an unreviewed external pair must reach native schema validation, not a blanket invalid-source diagnostic")
	require.Nil(t, response)
	require.NotContains(t, err.Error(), "unconfigured-provider")
	spec.Text = "MATCH ("
	response, err = pipeline.definitions.ValidateQuery(t.Context(), &contractpb.ValidateQueryRequest{
		Namespace: source.Namespace, Spec: spec,
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &contractpb.ValidateQueryResponse{Diagnostics: []*contractpb.Diagnostic{{
		Field: "spec.text", Message: "Query syntax or structure is invalid",
	}}}, response)
	after, err := watchservice.ReadDesiredView(t.Context(), pipeline.registry, pipeline.namespaceID, source.Namespace)
	require.NoError(t, err)
	require.Equal(t, before.Generation(), after.Generation(), "validation must not register or change a resource")
	_, err = pipeline.definitions.DeleteSource(t.Context(), &contractpb.SourceOperationRequest{
		Namespace: external.Namespace, Id: external.Id, ExpectedRevision: external.Revision, RequestId: tv.Any().String(),
	})
	require.NoError(t, err)
}

func (pipeline *watchHistoryPipeline) requireRunningResources(t *testing.T, source *contractpb.Source) {
	t.Helper()
	await.Require(t.Context(), t, func(t *await.T) {
		requireRunning := func(status *contractpb.ResourceStatus, revision uint64) {
			require.NotNil(t, status)
			require.NoError(t, status.UpdatedAt.CheckValid())
			require.LessOrEqual(t, time.Since(status.UpdatedAt.AsTime()), 5*time.Second)
			require.LessOrEqual(t, time.Until(status.UpdatedAt.AsTime()), 2*time.Second)
			protorequire.ProtoEqual(t, &contractpb.ResourceStatus{
				State:            contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING,
				ObservedRevision: revision, UpdatedAt: status.UpdatedAt,
			}, status)
		}
		observedSource, err := pipeline.definitions.GetSource(t.Context(), &contractpb.GetSourceRequest{Namespace: source.Namespace, Id: source.Id})
		require.NoError(t, err)
		requireRunning(observedSource.Status, source.Revision)
		observedQuery, err := pipeline.definitions.GetQuery(t.Context(), &contractpb.GetQueryRequest{Namespace: source.Namespace, Id: pipeline.query.Id})
		require.NoError(t, err)
		requireRunning(observedQuery.Status, pipeline.query.Revision)
		observedWatch, err := pipeline.definitions.GetWatch(t.Context(), &contractpb.GetWatchRequest{Namespace: source.Namespace, Id: pipeline.watch.Id})
		require.NoError(t, err)
		requireRunning(observedWatch.Status, pipeline.watch.Revision)
		sources, err := pipeline.definitions.ListSources(t.Context(), &contractpb.ListSourcesRequest{Namespace: source.Namespace})
		require.NoError(t, err)
		require.Len(t, sources.Sources, 1)
		requireRunning(sources.Sources[0].Status, source.Revision)
		queries, err := pipeline.definitions.ListQueries(t.Context(), &contractpb.ListQueriesRequest{Namespace: source.Namespace})
		require.NoError(t, err)
		queryIndex := slices.IndexFunc(queries.Queries, func(q *contractpb.Query) bool { return q.Id == pipeline.query.Id })
		require.NotEqual(t, -1, queryIndex)
		requireRunning(queries.Queries[queryIndex].Status, pipeline.query.Revision)
		watches, err := pipeline.definitions.ListWatches(t.Context(), &contractpb.ListWatchesRequest{Namespace: source.Namespace})
		require.NoError(t, err)
		watchIndex := slices.IndexFunc(watches.Watches, func(w *contractpb.Watch) bool { return w.Id == pipeline.watch.Id })
		require.NotEqual(t, -1, watchIndex)
		requireRunning(watches.Watches[watchIndex].Status, pipeline.watch.Revision)
	}, 30*time.Second, 100*time.Millisecond)
}

func workflowIDFromWatchResult(result json.RawMessage) (string, error) {
	var change contractpb.ResultChange
	if err := protojson.Unmarshal(result, &change); err != nil {
		return "", err
	}
	return change.GetAfter().GetFields()["workflow_id"].GetStringValue(), nil
}

func (pipeline *watchHistoryPipeline) executionID(t *testing.T, resource *contractpb.Watch) string {
	t.Helper()
	var id string
	await.Require(t.Context(), t, func(t *await.T) {
		view, err := watchservice.ReadDesiredView(t.Context(), pipeline.registry, pipeline.namespaceID, resource.Namespace)
		require.NoError(t, err)
		binding, err := view.NativeWatchBinding(resource.Id)
		require.NoError(t, err)
		encoded, err := json.Marshal(binding.Watch)
		require.NoError(t, err)
		id = fmt.Sprintf("w-%x", sha256.Sum256(append([]byte("temporal-native-watch-v1\x00"), encoded...)))
	}, 10*time.Second, 10*time.Millisecond)
	return id
}

func (pipeline *watchHistoryPipeline) verifyConditions(t *testing.T, producerID string) {
	t.Helper()
	require.True(t, t.Run("conditions", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			closed  bool
			waiting bool
			status  controlpb.Status
		}{
			{"matched", false, false, controlpb.STATUS_SATISFIED},
			{"closed-owner", true, false, controlpb.STATUS_CANCELLED},
			{"owner-closes", true, true, controlpb.STATUS_CANCELLED},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ctx := testcontext.For(t)
				tv := testvars.New(t)
				owner, err := pipeline.client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID: tv.WorkflowID(), TaskQueue: pipeline.taskQueue,
				}, pipeline.conditionType)
				require.NoError(t, err)
				if tc.closed && !tc.waiting {
					require.NoError(t, pipeline.client.TerminateWorkflow(ctx, owner.GetID(), owner.GetRunID(), "condition verification"))
				}
				query := pipeline.query
				if tc.waiting {
					spec := common.CloneProto(query.Spec)
					spec.Text = "MATCH (w:Workflow) WHERE false RETURN w.workflow_id AS workflow_id"
					query, err = pipeline.definitions.CreateQuery(ctx, &contractpb.CreateQueryRequest{
						Namespace: query.Namespace, Id: "runtime.condition.empty", Spec: spec, RequestId: tv.Any().String(),
					})
					require.NoError(t, err)
					query, err = pipeline.definitions.StartQuery(ctx, &contractpb.QueryOperationRequest{
						Namespace: query.Namespace, Id: query.Id, ExpectedRevision: query.Revision, RequestId: tv.Any().String(),
					})
					require.NoError(t, err)
				}
				execution := &commonpb.WorkflowExecution{WorkflowId: owner.GetID(), RunId: owner.GetRunID()}
				resource, err := pipeline.definitions.CreateWatch(ctx, &contractpb.CreateWatchRequest{
					Namespace: pipeline.query.Namespace, Id: "runtime.condition." + tc.name, RequestId: tv.Any().String(),
					Spec: &contractpb.WatchSpec{
						Query:   &contractpb.ResourceRef{Id: query.Id, Revision: query.Revision},
						Trigger: contractpb.Trigger_TRIGGER_ENTER,
						Lifetime: &contractpb.WatchSpec_Once{Once: &contractpb.Once{
							Owner: execution, Deadline: timestamppb.New(time.Now().Add(5 * time.Minute)),
						}},
						Action: &contractpb.WorkflowAction{Operation: &contractpb.WorkflowAction_Signal{Signal: &contractpb.WorkflowSignal{
							Target: &contractpb.WorkflowSignal_Run{Run: execution}, SignalName: "condition",
						}}},
					},
				})
				require.NoError(t, err)
				id := pipeline.executionID(t, resource)
				if tc.waiting {
					await.Require(ctx, t, func(t *await.T) {
						response, err := pipeline.control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
							NamespaceId: pipeline.namespaceID, ExecutionId: id,
						})
						require.NoError(t, err)
						require.Equal(t, controlpb.STATUS_RUNNING, response.GetState().GetStatus())
						require.Equal(t, controlpb.PROGRESS_PHASE_LIVE, response.GetState().GetProgressPhase())
					}, 30*time.Second, 100*time.Millisecond)
					require.NoError(t, pipeline.client.TerminateWorkflow(ctx, owner.GetID(), owner.GetRunID(), "condition verification"))
				}
				var state *controlpb.State
				await.Require(ctx, t, func(t *await.T) {
					response, err := pipeline.control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
						NamespaceId: pipeline.namespaceID, ExecutionId: id,
					})
					require.NoError(t, err)
					state = response.GetState()
					require.NotNil(t, state)
					require.True(t, state.Status == controlpb.STATUS_SATISFIED || state.Status == controlpb.STATUS_CANCELLED)
					require.Empty(t, state.PendingIds)
				}, 30*time.Second, 100*time.Millisecond)
				require.Equal(t, tc.status, state.Status)
				await.Require(ctx, t, func(t *await.T) {
					current, err := pipeline.definitions.GetWatch(t.Context(), &contractpb.GetWatchRequest{
						Namespace: resource.Namespace, Id: resource.Id,
					})
					require.NoError(t, err)
					expected := contractpb.LifecycleState_LIFECYCLE_STATE_COMPLETED
					if tc.closed {
						expected = contractpb.LifecycleState_LIFECYCLE_STATE_CANCELED
					}
					require.Equal(t, expected, current.GetStatus().GetState())
					require.Equal(t, resource.Revision, current.Status.ObservedRevision)
					require.Zero(t, current.Status.PendingDeliveries)
					require.Nil(t, current.Status.AcceptedOutput)
				}, 30*time.Second, 100*time.Millisecond)
				require.Zero(t, state.Failed)
				if tc.closed {
					require.Zero(t, state.Delivered)
				} else {
					require.Equal(t, uint64(1), state.Delivered)
					var result string
					require.NoError(t, owner.Get(ctx, &result))
					require.Equal(t, producerID, result)
				}
			})
		}
	}))
}

func (pipeline *watchHistoryPipeline) completeProducer(t *testing.T, id string) string {
	t.Helper()
	ctx := testcontext.For(t)
	producer, err := pipeline.client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: id, TaskQueue: pipeline.taskQueue,
	}, pipeline.producerType)
	require.NoError(t, err)
	require.NoError(t, pipeline.client.SignalWorkflow(ctx, producer.GetID(), producer.GetRunID(), "finish", true))
	require.NoError(t, producer.Get(ctx, nil))
	return producer.GetID()
}

func (pipeline *watchHistoryPipeline) requireDelivery(t *testing.T, delivered uint64, producerID string) {
	t.Helper()
	ctx := testcontext.For(t)
	await.Require(ctx, t, func(t *await.T) {
		response, err := pipeline.control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
			NamespaceId: pipeline.namespaceID, ExecutionId: pipeline.controlID,
		})
		require.NoError(t, err)
		require.Equal(t, delivered, response.State.Delivered)
		require.Zero(t, response.State.Failed)
		require.Empty(t, response.State.PendingIds)
		public, err := pipeline.definitions.ListDeliveries(t.Context(), &contractpb.ListDeliveriesRequest{
			Namespace: pipeline.watch.Namespace, Id: pipeline.watch.Id,
		})
		require.NoError(t, err)
		require.Len(t, public.Deliveries, int(delivered))
		require.Empty(t, public.NextPageToken)
		for _, delivery := range public.Deliveries {
			require.Equal(t, contractpb.DeliveryState_DELIVERY_STATE_DELIVERED, delivery.State)
			require.Equal(t, pipeline.watch.Id, delivery.Watch.Id)
			require.Equal(t, pipeline.watch.Revision, delivery.Watch.Revision)
			require.Equal(t, pipeline.query.Revision, delivery.Output.QueryRevision)
			require.Equal(t, response.State.Incarnation, delivery.Output.Incarnation)
			require.Equal(t, pipeline.consumerID, delivery.Execution.WorkflowId)
			require.NotEmpty(t, delivery.Execution.RunId)
			require.NoError(t, delivery.UpdatedAt.CheckValid())
		}
	}, 30*time.Second, 100*time.Millisecond)
	var result string
	require.NoError(t, pipeline.client.GetWorkflow(ctx, pipeline.consumerID, "").Get(ctx, &result))
	require.Equal(t, producerID, result)
}
