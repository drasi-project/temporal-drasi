//go:build integration && temporal_with_drasi && cgo

package tests

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/service/watch/native"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
)

type graphControlGate struct {
	controlpb.ControlServiceClient
	mu             sync.Mutex
	blockExecution string
	release        chan struct{}
	entered        chan struct{}
	loseAcceptance bool
	accepted       chan struct{}
}

func (c *graphControlGate) MutateExecution(ctx context.Context, request *controlpb.MutateExecutionRequest, options ...grpc.CallOption) (*controlpb.ExecutionResponse, error) {
	c.mu.Lock()
	blocked := request.ExecutionId == c.blockExecution && request.GetPause().GetPaused()
	release, entered := c.release, c.entered
	c.mu.Unlock()
	if blocked {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
		}
	}
	return c.ControlServiceClient.MutateExecution(ctx, request, options...)
}

func (c *graphControlGate) AcceptOutput(ctx context.Context, request *controlpb.AcceptOutputRequest, options ...grpc.CallOption) (*controlpb.AcceptOutputResponse, error) {
	response, err := c.ControlServiceClient.AcceptOutput(ctx, request, options...)
	c.mu.Lock()
	lost, accepted := c.loseAcceptance, c.accepted
	c.mu.Unlock()
	if err == nil && lost && !request.Initial {
		select {
		case accepted <- struct{}{}:
		default:
		}
		return nil, serviceerror.NewUnavailable("acceptance response lost after commit")
	}
	return response, err
}

func TestDrasiGraphReconcilesDefinitionsAndRetainedAliases(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableDrasiWatches, true))
	ctx := testcontext.For(t)
	tv := testvars.New(t)
	definitions := env.GetTestCluster().DrasiClient()
	registry := env.GetTestCluster().WatchRegistryClient()
	control := &graphControlGate{
		ControlServiceClient: env.GetTestCluster().WatchControlClient(),
		release:              make(chan struct{}), entered: make(chan struct{}, 1), accepted: make(chan struct{}, 1),
	}
	env.SdkWorker().RegisterWorkflowWithOptions(func(_ workflow.Context, result json.RawMessage) (string, error) {
		var change watchpb.ResultChange
		if err := protojson.Unmarshal(result, &change); err != nil {
			return "", err
		}
		return change.GetAfter().GetFields()["id"].GetStringValue(), nil
	}, workflow.RegisterOptions{Name: tv.WorkflowType().Name})
	createSource := func(id string) *watchpb.Source {
		source, err := definitions.CreateSource(ctx, &watchpb.CreateSourceRequest{
			Namespace: env.Namespace().String(), Id: id, RequestId: id + "-create",
			Spec: &watchpb.SourceSpec{Kind: &watchpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}},
		})
		require.NoError(t, err)
		source, err = definitions.StartSource(ctx, &watchpb.SourceOperationRequest{
			Namespace: source.Namespace, Id: id, ExpectedRevision: source.Revision, RequestId: id + "-start",
		})
		require.NoError(t, err)
		return source
	}
	createQuery := func(id string, source *watchpb.Source) *watchpb.Query {
		query, err := definitions.CreateQuery(ctx, &watchpb.CreateQueryRequest{
			Namespace: env.Namespace().String(), Id: id, RequestId: id + "-create",
			Spec: &watchpb.QuerySpec{
				Language: "cypher", Text: "MATCH (n:Item) RETURN n.id AS id", StateStore: "persistent",
				Sources: []*watchpb.ResourceRef{{Id: source.Id, Revision: source.Revision}},
			},
		})
		require.NoError(t, err)
		query, err = definitions.StartQuery(ctx, &watchpb.QueryOperationRequest{
			Namespace: query.Namespace, Id: id, ExpectedRevision: query.Revision, RequestId: id + "-start",
		})
		require.NoError(t, err)
		return query
	}
	createWatch := func(id string, query *watchpb.Query) *watchpb.Watch {
		watch, err := definitions.CreateWatch(ctx, &watchpb.CreateWatchRequest{
			Namespace: env.Namespace().String(), Id: id, RequestId: id + "-create",
			Spec: &watchpb.WatchSpec{
				Query: &watchpb.ResourceRef{Id: query.Id, Revision: query.Revision}, Trigger: watchpb.Trigger_TRIGGER_ENTER,
				Lifetime: &watchpb.WatchSpec_Persistent{Persistent: &emptypb.Empty{}},
				Action: &watchpb.WorkflowAction{Operation: &watchpb.WorkflowAction_Start{Start: &watchpb.WorkflowStart{
					WorkflowId: "{action_id}", WorkflowType: tv.WorkflowType().Name, TaskQueue: env.WorkerTaskQueue(),
					ReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				}}},
			},
		})
		require.NoError(t, err)
		return watch
	}
	firstSource := createSource("source.aliases")
	firstQuery := createQuery("query.aliases", firstSource)
	firstWatch := createWatch("watch.original", firstQuery)
	healthySource := createSource("source.healthy")
	healthyQuery := createQuery("query.healthy", healthySource)
	healthyWatch := createWatch("watch.healthy", healthyQuery)
	secondSource, err := definitions.UpdateSource(ctx, &watchpb.UpdateSourceRequest{
		Namespace: firstSource.Namespace, Id: firstSource.Id, ExpectedRevision: firstSource.Revision,
		RequestId: "source-new-lifetime", Spec: common.CloneProto(firstSource.Spec),
	})
	require.NoError(t, err)
	secondQuery, err := definitions.UpdateQuery(ctx, &watchpb.UpdateQueryRequest{
		Namespace: firstQuery.Namespace, Id: firstQuery.Id, ExpectedRevision: firstQuery.Revision,
		RequestId: "query-new-lifetime", Spec: &watchpb.QuerySpec{
			Language: "cypher", Text: firstQuery.Spec.Text, StateStore: "persistent",
			Sources: []*watchpb.ResourceRef{{Id: secondSource.Id, Revision: secondSource.Revision}},
		},
	})
	require.NoError(t, err)
	createWatch("watch.successor", secondQuery)
	view, err := watchservice.ReadDesiredView(ctx, registry, env.NamespaceID().String(), env.Namespace().String())
	require.NoError(t, err)
	originalView := view
	options := watchservice.GraphOptions{
		TemporalInput: watchservice.ManualTemporalInput{},
		NamespaceID:   env.NamespaceID().String(), Namespace: env.Namespace().String(), StateRoot: t.TempDir(),
		StorageHostID: "10000000-0000-4000-8000-000000000001",
		ProcessBootID: "20000000-0000-4000-8000-000000000001", RequestID: "reconciler-first",
		Assignments: env.GetTestCluster().WatchAssignmentClient(), Registry: registry,
		Control: control, Workflows: env.GetTestCluster().FrontendClient(),
	}
	graph, err := watchservice.OpenGraph(ctx, options)
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, graph.Close(shutdown))
	})
	reconcile := func() watchservice.GraphObservation {
		var observation watchservice.GraphObservation
		await.Require(ctx, t, func(t *await.T) {
			var err error
			observation, err = graph.Reconcile(t.Context(), view)
			require.NoError(t, err)
			require.Empty(t, observation.Problems)
			require.True(t, observation.Converged, "%+v", observation)
		}, 20*time.Second, 50*time.Millisecond)
		return observation
	}
	observation := reconcile()
	require.Len(t, observation.Nodes, 6)
	require.Len(t, observation.Watches, 3)
	var firstID, healthyID string
	for _, watch := range observation.Watches {
		require.True(t, watch.Accepting)
		switch watch.Lifetime.ResourceID {
		case firstWatch.Id:
			firstID = watch.SubscriptionID
		case healthyWatch.Id:
			healthyID = watch.SubscriptionID
		default:
			continue
		}
	}
	require.NotEmpty(t, firstID)
	require.NotEmpty(t, healthyID)
	sourceLifetime := func(source *watchpb.Source) native.ResourceLifetime {
		record, err := view.Source(&watchpb.ResourceRef{Id: source.Id, Revision: source.Revision})
		require.NoError(t, err)
		return native.ResourceLifetime{NamespaceID: options.NamespaceID, ResourceID: source.Id, Incarnation: record.Incarnation}
	}
	originalSourceLifetime, healthySourceLifetime := sourceLifetime(firstSource), sourceLifetime(healthySource)
	successorSourceLifetime := sourceLifetime(secondSource)
	originalQueryRecord, err := view.Query(&watchpb.ResourceRef{Id: firstQuery.Id, Revision: firstQuery.Revision})
	require.NoError(t, err)
	originalQueryLifetime := native.ResourceLifetime{
		NamespaceID: options.NamespaceID, ResourceID: firstQuery.Id, Incarnation: originalQueryRecord.Incarnation,
	}
	control.mu.Lock()
	control.blockExecution = firstID
	control.mu.Unlock()
	stopped, err := definitions.StopSource(ctx, &watchpb.SourceOperationRequest{
		Namespace: secondSource.Namespace, Id: secondSource.Id, ExpectedRevision: secondSource.Revision, RequestId: "stop-all-aliases",
	})
	require.NoError(t, err)
	view, err = watchservice.ReadDesiredView(ctx, registry, options.NamespaceID, options.Namespace)
	require.NoError(t, err)
	_, err = graph.Reconcile(ctx, view)
	require.NoError(t, err)
	await.Rcv(t, control.entered)
	waiting, err := graph.Reconcile(ctx, view)
	require.NoError(t, err)
	for _, node := range waiting.Nodes {
		if node.Definition.Lifetime.ResourceID == firstQuery.Id {
			require.Equal(t, native.NodeRunning, node.Status, "physical query stop must wait for History pause")
		}
	}
	require.NoError(t, graph.FeedTemporal(ctx, healthySourceLifetime, 1, native.Change{
		Kind: native.Insert, ElementID: "healthy", Labels: []string{"Item"}, EffectiveFrom: 1,
		Properties: map[string]any{"id": "unrelated-progress"},
	}))
	var healthyState *controlpb.ExecutionResponse
	await.Require(ctx, t, func(t *await.T) {
		var err error
		healthyState, err = control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
			NamespaceId: options.NamespaceID, ExecutionId: healthyID,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1), healthyState.State.Delivered)
	}, 10*time.Second, 50*time.Millisecond)
	var result string
	require.NoError(t, env.SdkClient().GetWorkflow(ctx, healthyState.Deliveries[0].WorkflowId, "").Get(ctx, &result))
	require.Equal(t, "unrelated-progress", result)
	close(control.release)
	observation = reconcile()
	for _, node := range observation.Nodes {
		if node.Definition.Lifetime.ResourceID == firstSource.Id || node.Definition.Lifetime.ResourceID == firstQuery.Id {
			require.Equal(t, native.NodeStopped, node.Status)
		}
	}
	_, err = definitions.StartSource(ctx, &watchpb.SourceOperationRequest{
		Namespace: stopped.Namespace, Id: stopped.Id, ExpectedRevision: stopped.Revision, RequestId: "resume-all-aliases",
	})
	require.NoError(t, err)
	view, err = watchservice.ReadDesiredView(ctx, registry, options.NamespaceID, options.Namespace)
	require.NoError(t, err)
	observation = reconcile()
	for _, node := range observation.Nodes {
		require.Equal(t, native.NodeRunning, node.Status)
	}
	_, err = graph.Reconcile(ctx, originalView)
	require.ErrorContains(t, err, "regressed")
	paused, err := definitions.PauseWatch(ctx, &watchpb.WatchOperationRequest{
		Namespace: firstWatch.Namespace, Id: firstWatch.Id, ExpectedRevision: firstWatch.Revision, RequestId: "pause-original-definition",
	})
	require.NoError(t, err)
	resumed, err := definitions.ResumeWatch(ctx, &watchpb.WatchOperationRequest{
		Namespace: paused.Namespace, Id: paused.Id, ExpectedRevision: paused.Revision, RequestId: "resume-original-definition",
	})
	require.NoError(t, err)
	view, err = watchservice.ReadDesiredView(ctx, registry, options.NamespaceID, options.Namespace)
	require.NoError(t, err)
	reconcile()
	control.mu.Lock()
	control.loseAcceptance = true
	control.mu.Unlock()
	require.NoError(t, graph.FeedTemporal(ctx, originalSourceLifetime, 1, native.Change{
		Kind: native.Insert, ElementID: "original", Labels: []string{"Item"}, EffectiveFrom: 1,
		Properties: map[string]any{"id": "accepted-before-close"},
	}))
	await.Rcv(t, control.accepted)
	require.NoError(t, graph.Close(ctx))
	options.ProcessBootID, options.RequestID = "20000000-0000-4000-8000-000000000002", "reconciler-successor"
	control.mu.Lock()
	control.loseAcceptance = false
	control.mu.Unlock()
	graph, err = watchservice.OpenGraph(ctx, options)
	require.NoError(t, err)
	reconcile()
	await.Require(ctx, t, func(t *await.T) {
		state, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
			NamespaceId: options.NamespaceID, ExecutionId: firstID,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(2), state.State.OwnerEpoch)
		require.Equal(t, firstWatch.Revision, state.State.CreationRevision)
		require.Equal(t, uint64(1), state.State.Delivered)
		require.Empty(t, state.State.PendingIds)
		require.Len(t, state.Deliveries, 1)
		require.NoError(t, env.SdkClient().GetWorkflow(t.Context(), state.Deliveries[0].WorkflowId, "").Get(t.Context(), &result))
		require.Equal(t, "accepted-before-close", result)
	}, 10*time.Second, 50*time.Millisecond)
	replacement, err := definitions.UpdateWatch(ctx, &watchpb.UpdateWatchRequest{
		Namespace: resumed.Namespace, Id: resumed.Id, ExpectedRevision: resumed.Revision, RequestId: "replace-original-watch",
		Spec: common.CloneProto(firstWatch.Spec),
	})
	require.NoError(t, err)
	view, err = watchservice.ReadDesiredView(ctx, registry, options.NamespaceID, options.Namespace)
	require.NoError(t, err)
	observation = reconcile()
	var replacementID string
	for _, watch := range observation.Watches {
		if watch.SubscriptionID == firstID {
			require.Equal(t, controlpb.STATUS_CANCELLED, watch.Status)
			require.True(t, watch.Quiesced)
			require.False(t, watch.Accepting)
		} else if watch.Lifetime.ResourceID == firstWatch.Id {
			replacementID = watch.SubscriptionID
			require.True(t, watch.Accepting)
		}
	}
	require.NotEmpty(t, replacementID)
	require.NotEqual(t, firstID, replacementID)
	require.NoError(t, graph.FeedTemporal(ctx, originalSourceLifetime, 2, native.Change{
		Kind: native.Insert, ElementID: "replacement", Labels: []string{"Item"}, EffectiveFrom: 2,
		Properties: map[string]any{"id": "new-watch-only"},
	}))
	await.Require(ctx, t, func(t *await.T) {
		old, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
			NamespaceId: options.NamespaceID, ExecutionId: firstID,
		})
		require.NoError(t, err)
		require.Equal(t, controlpb.STATUS_CANCELLED, old.State.Status)
		require.Equal(t, uint64(1), old.State.Delivered)
		current, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
			NamespaceId: options.NamespaceID, ExecutionId: replacementID,
		})
		require.NoError(t, err)
		require.Equal(t, replacement.Revision, current.State.CreationRevision)
		require.Equal(t, uint64(1), current.State.Delivered)
		require.Len(t, current.Deliveries, 1)
		require.NoError(t, env.SdkClient().GetWorkflow(t.Context(), current.Deliveries[0].WorkflowId, "").Get(t.Context(), &result))
		require.Equal(t, "new-watch-only", result)
	}, 10*time.Second, 50*time.Millisecond)
	drained := map[string]uint64{firstID: 1}
	for _, tc := range []struct {
		name      string
		sequence  uint64
		delivered uint64
		mutate    func(context.Context, *watchpb.Watch) (*watchpb.Watch, error)
	}{
		{
			name: "edit", sequence: 3, delivered: 1,
			mutate: func(ctx context.Context, resource *watchpb.Watch) (*watchpb.Watch, error) {
				spec := common.CloneProto(resource.Spec)
				spec.Action.GetStart().WorkflowId = "edited-{action_id}"
				return definitions.UpdateWatch(ctx, &watchpb.UpdateWatchRequest{
					Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision,
					RequestId: "edit-with-pending-delivery", Spec: spec,
				})
			},
		},
		{
			name: "delete", sequence: 4, delivered: 0,
			mutate: func(ctx context.Context, resource *watchpb.Watch) (*watchpb.Watch, error) {
				return definitions.DeleteWatch(ctx, &watchpb.WatchOperationRequest{
					Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision,
					RequestId: "delete-with-pending-delivery",
				})
			},
		},
	} {
		require.True(t, t.Run(tc.name, func(t *testing.T) {
			ctx := testcontext.For(t)
			oldID, oldRevision := replacementID, replacement.Revision
			removeFailure := env.InjectRequestFault(func(request any) error {
				if request, ok := request.(*workflowservice.StartWorkflowExecutionRequest); ok &&
					request.GetWorkflowType().GetName() == tv.WorkflowType().Name {
					return serviceerror.NewUnavailable("hold accepted delivery until lifecycle mutation")
				}
				return nil
			})
			require.NoError(t, graph.FeedTemporal(ctx, originalSourceLifetime, tc.sequence, native.Change{
				Kind: native.Insert, ElementID: "pending-" + tc.name, Labels: []string{"Item"}, EffectiveFrom: 1,
				Properties: map[string]any{"id": tc.name},
			}))
			var pending *controlpb.Intent
			await.Require(ctx, t, func(t *await.T) {
				state, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
					NamespaceId: options.NamespaceID, ExecutionId: oldID,
				})
				require.NoError(t, err)
				require.Equal(t, tc.delivered, state.State.Delivered)
				require.Len(t, state.State.PendingIds, 1)
				for _, delivery := range state.Deliveries {
					if delivery.ActionId == state.State.PendingIds[0] {
						pending = common.CloneProto(delivery)
					}
				}
				require.NotNil(t, pending)
				require.Positive(t, pending.Attempts)
			}, 15*time.Second, 50*time.Millisecond)
			replacement, err = tc.mutate(ctx, replacement)
			require.NoError(t, err)
			view, err = watchservice.ReadDesiredView(ctx, registry, options.NamespaceID, options.Namespace)
			require.NoError(t, err)
			await.Require(ctx, t, func(t *await.T) {
				observation, err := graph.Reconcile(t.Context(), view)
				require.NoError(t, err)
				require.Empty(t, observation.Problems)
				if tc.name == "delete" {
					var retired []native.ResourceLifetime
					for _, node := range observation.Nodes {
						if node.Definition.Lifetime == originalQueryLifetime || node.Definition.Lifetime == originalSourceLifetime {
							require.Equal(t, native.NodeRetired, node.Status)
							retired = append(retired, node.Definition.Lifetime)
						}
					}
					require.ElementsMatch(t, []native.ResourceLifetime{originalQueryLifetime, originalSourceLifetime}, retired)
				}
				if replacement.DesiredState == watchpb.DesiredState_DESIRED_STATE_RUNNING {
					var nextID string
					for _, watch := range observation.Watches {
						if watch.Lifetime.ResourceID == replacement.Id && watch.Accepting {
							nextID = watch.SubscriptionID
						}
					}
					require.NotEmpty(t, nextID)
					require.NotEqual(t, oldID, nextID)
					current, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
						NamespaceId: options.NamespaceID, ExecutionId: nextID,
					})
					require.NoError(t, err)
					require.Equal(t, controlpb.PROGRESS_PHASE_LIVE, current.State.ProgressPhase)
					require.Zero(t, current.State.Delivered)
					replacementID = nextID
				}
				state, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
					NamespaceId: options.NamespaceID, ExecutionId: oldID,
				})
				require.NoError(t, err)
				require.Equal(t, controlpb.STATUS_CANCELLED, state.State.Status)
				require.Equal(t, []string{pending.ActionId}, state.State.PendingIds)
				for _, delivery := range state.Deliveries {
					if delivery.ActionId == pending.ActionId {
						require.Equal(t, pending.Request, delivery.Request)
					}
				}
			}, 15*time.Second, 50*time.Millisecond)
			removeFailure()
			await.Require(ctx, t, func(t *await.T) {
				state, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
					NamespaceId: options.NamespaceID, ExecutionId: oldID,
				})
				require.NoError(t, err)
				require.Equal(t, controlpb.STATUS_CANCELLED, state.State.Status)
				require.Equal(t, oldRevision, state.State.CreationRevision)
				require.Equal(t, tc.delivered+1, state.State.Delivered)
				require.Empty(t, state.State.PendingIds)
				require.Zero(t, state.State.Failed)
				for _, delivery := range state.Deliveries {
					if delivery.ActionId == pending.ActionId {
						require.Equal(t, pending.WorkflowId, delivery.WorkflowId)
					}
				}
			}, 15*time.Second, 50*time.Millisecond)
			var result string
			require.NoError(t, env.SdkClient().GetWorkflow(ctx, pending.WorkflowId, "").Get(ctx, &result))
			require.Equal(t, tc.name, result)
			drained[oldID] = tc.delivered + 1
		}))
	}
	_, err = definitions.CreateWatch(ctx, &watchpb.CreateWatchRequest{
		Namespace: env.Namespace().String(), Id: "watch.stale-dependency", RequestId: "reject-retired-query",
		Spec: common.CloneProto(firstWatch.Spec),
	})
	require.ErrorContains(t, err, "superseded resource definition")
	probe := createWatch("watch.after-delete", secondQuery)
	view, err = watchservice.ReadDesiredView(ctx, registry, options.NamespaceID, options.Namespace)
	require.NoError(t, err)
	observation = reconcile()
	var probeID string
	for _, watch := range observation.Watches {
		if watch.Lifetime.ResourceID == probe.Id {
			probeID = watch.SubscriptionID
			require.True(t, watch.Accepting)
		}
	}
	require.NotEmpty(t, probeID)
	require.NoError(t, graph.FeedTemporal(ctx, successorSourceLifetime, 1, native.Change{
		Kind: native.Insert, ElementID: "after-delete", Labels: []string{"Item"}, EffectiveFrom: 1,
		Properties: map[string]any{"id": "after-delete"},
	}))
	await.Require(ctx, t, func(t *await.T) {
		state, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
			NamespaceId: options.NamespaceID, ExecutionId: probeID,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1), state.State.Delivered)
		require.Empty(t, state.State.PendingIds)
	}, 15*time.Second, 50*time.Millisecond)
	for id, delivered := range drained {
		state, err := control.DescribeExecution(ctx, &controlpb.DescribeExecutionRequest{
			NamespaceId: options.NamespaceID, ExecutionId: id,
		})
		require.NoError(t, err)
		require.Equal(t, controlpb.STATUS_CANCELLED, state.State.Status)
		require.Equal(t, delivered, state.State.Delivered)
		require.Empty(t, state.State.PendingIds)
	}
	require.NoError(t, graph.Close(ctx))
	options.ProcessBootID, options.RequestID = "20000000-0000-4000-8000-000000000003", "reconciler-retired-successor"
	graph, err = watchservice.OpenGraph(ctx, options)
	require.NoError(t, err)
	observation = reconcile()
	var retired []native.ResourceLifetime
	for _, node := range observation.Nodes {
		if node.Definition.Lifetime == originalQueryLifetime || node.Definition.Lifetime == originalSourceLifetime {
			require.Equal(t, native.NodeRetired, node.Status)
			retired = append(retired, node.Definition.Lifetime)
		}
	}
	require.ElementsMatch(t, []native.ResourceLifetime{originalQueryLifetime, originalSourceLifetime}, retired)
	state, err := control.DescribeExecution(ctx, &controlpb.DescribeExecutionRequest{
		NamespaceId: options.NamespaceID, ExecutionId: probeID,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.State.Delivered)
	require.Empty(t, state.State.PendingIds)
}
