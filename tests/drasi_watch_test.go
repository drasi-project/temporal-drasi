//go:build integration && temporal_with_drasi && cgo

package tests

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/workflow"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	watchservice "go.temporal.io/server/service/watch"
	"go.temporal.io/server/service/watch/native"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type registryAfterReadClient struct {
	registrypb.RegistryServiceClient
	after func() error
}

func (c *registryAfterReadClient) Read(
	ctx context.Context,
	request *registrypb.ReadRequest,
	options ...grpc.CallOption,
) (*registrypb.ReadResponse, error) {
	response, err := c.RegistryServiceClient.Read(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	if c.after != nil {
		after := c.after
		c.after = nil
		if err := after(); err != nil {
			return nil, err
		}
	}
	return response, nil
}

func TestDrasiNativeOutputStartsWorkflow(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableDrasiWatches, true))
	tv := testvars.New(t)
	ctx := testcontext.For(t)
	control := env.GetTestCluster().WatchControlClient()
	registry := env.GetTestCluster().WatchRegistryClient()
	definitions := env.GetTestCluster().DrasiClient()
	capabilities, err := definitions.GetCapabilities(ctx, &watchpb.GetCapabilitiesRequest{Namespace: env.Namespace().String()})
	require.NoError(t, err)
	require.True(t, capabilities.Enabled)
	require.Contains(t, capabilities.AllowedMethods, watchpb.WatchService_CreateSource_FullMethodName)
	require.NotContains(t, capabilities.AllowedMethods, watchpb.WatchService_PreviewQuery_FullMethodName)
	require.False(t, capabilities.TemporalSource)
	require.False(t, capabilities.ExternalSources)
	sourceCreated, err := definitions.CreateSource(ctx, &watchpb.CreateSourceRequest{
		Namespace: env.Namespace().String(), Id: "feed.events", RequestId: tv.RequestID() + "-source",
		Spec: &watchpb.SourceSpec{Kind: &watchpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}},
	})
	require.NoError(t, err)
	require.Equal(t, watchpb.DesiredState_DESIRED_STATE_STOPPED, sourceCreated.DesiredState)
	require.Nil(t, sourceCreated.Status)
	source, err := definitions.StartSource(ctx, &watchpb.SourceOperationRequest{
		Namespace: env.Namespace().String(), Id: "feed.events", RequestId: tv.RequestID() + "-start-source",
		ExpectedRevision: sourceCreated.Revision,
	})
	require.NoError(t, err)
	require.Equal(t, watchpb.DesiredState_DESIRED_STATE_RUNNING, source.DesiredState)
	require.Nil(t, source.Status)
	queryCreated, err := definitions.CreateQuery(ctx, &watchpb.CreateQueryRequest{
		Namespace: env.Namespace().String(), Id: "saved.query", RequestId: tv.RequestID() + "-query",
		Spec: &watchpb.QuerySpec{
			Language: "cypher", Text: "MATCH (n:Item) WHERE n.amount >= 10 RETURN n.id AS id",
			StateStore: "persistent", Sources: []*watchpb.ResourceRef{{Id: source.Id, Revision: source.Revision}},
		},
	})
	require.NoError(t, err)
	query, err := definitions.StartQuery(ctx, &watchpb.QueryOperationRequest{
		Namespace: env.Namespace().String(), Id: "saved.query", RequestId: tv.RequestID() + "-start-query",
		ExpectedRevision: queryCreated.Revision,
	})
	require.NoError(t, err)
	require.Equal(t, watchpb.DesiredState_DESIRED_STATE_RUNNING, query.DesiredState)
	require.Nil(t, query.Status)
	worker := env.SdkWorker()
	worker.RegisterWorkflowWithOptions(func(_ workflow.Context, result json.RawMessage) (string, error) {
		var change watchpb.ResultChange
		if err := protojson.Unmarshal(result, &change); err != nil {
			return "", err
		}
		return change.GetAfter().GetFields()["id"].GetStringValue(), nil
	}, workflow.RegisterOptions{Name: tv.WorkflowType().Name})
	assignments := &assignmentLostSealClient{AssignmentServiceClient: env.GetTestCluster().WatchAssignmentClient()}
	opening := &assignmentpb.BeginRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: watchassignment.BusinessID, RequestId: tv.RequestID() + "-owner",
		StorageHostId: "10000000-0000-4000-8000-000000000001",
		ProcessBootId: "20000000-0000-4000-8000-000000000001",
	}
	owner, err := assignments.Begin(ctx, opening)
	require.NoError(t, err)
	nextOpening := common.CloneProto(opening)
	nextOpening.RequestId += "-next"
	nextOpening.ProcessBootId = "20000000-0000-4000-8000-000000000002"
	nextOpening.ExpectedVersion = owner.Version
	_, err = assignments.Begin(ctx, nextOpening)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	nativeConfig := native.Config{
		StateDir:   filepath.Join(t.TempDir(), "native"),
		OwnerEpoch: owner.GetActive().Grant.Epoch, InstanceID: owner.GetActive().Grant.InstanceId,
	}
	runtime, err := native.Open(ctx, nativeConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, runtime.Close(ctx))
	})
	createWatch := &watchpb.CreateWatchRequest{
		Namespace: env.Namespace().String(), Id: tv.WorkflowID(), RequestId: tv.RequestID() + "-watch",
		Spec: &watchpb.WatchSpec{
			Query: &watchpb.ResourceRef{Id: query.Id, Revision: query.Revision}, Trigger: watchpb.Trigger_TRIGGER_ENTER,
			Lifetime: &watchpb.WatchSpec_Persistent{Persistent: &emptypb.Empty{}},
			Action: &watchpb.WorkflowAction{Operation: &watchpb.WorkflowAction_Start{
				Start: &watchpb.WorkflowStart{
					WorkflowId: tv.WithWorkflowIDNumber(1).WorkflowID(), WorkflowType: tv.WorkflowType().Name,
					TaskQueue: env.WorkerTaskQueue(), ReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				},
			}},
		},
	}
	resource, err := definitions.CreateWatch(ctx, createWatch)
	require.NoError(t, err)
	retried, err := definitions.CreateWatch(ctx, createWatch)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, resource, retried)
	require.Equal(t, watchpb.DesiredState_DESIRED_STATE_RUNNING, resource.DesiredState)
	require.Nil(t, resource.Status)
	watches, err := definitions.ListWatches(ctx, &watchpb.ListWatchesRequest{Namespace: env.Namespace().String()})
	require.NoError(t, err)
	unhosted := common.CloneProto(resource)
	unhosted.Status = &watchpb.ResourceStatus{Message: "Runtime observation unavailable"}
	protorequire.ProtoEqual(t, &watchpb.ListWatchesResponse{Watches: []*watchpb.Watch{unhosted}}, watches)
	changingRegistry := &registryAfterReadClient{RegistryServiceClient: registry, after: func() error {
		_, err := definitions.StartSource(ctx, &watchpb.SourceOperationRequest{
			Namespace: source.Namespace, Id: source.Id, ExpectedRevision: source.Revision,
			RequestId: tv.RequestID() + "-source-lifecycle",
		})
		if err != nil {
			return err
		}
		_, err = definitions.StartQuery(ctx, &watchpb.QueryOperationRequest{
			Namespace: query.Namespace, Id: query.Id, ExpectedRevision: query.Revision,
			RequestId: tv.RequestID() + "-query-lifecycle",
		})
		return err
	}}
	mixed, err := watchservice.ReadDesiredView(ctx, changingRegistry, env.NamespaceID().String(), env.Namespace().String())
	require.ErrorContains(t, err, "registry changed")
	require.Nil(t, mixed)
	view, err := watchservice.ReadDesiredView(ctx, registry, env.NamespaceID().String(), env.Namespace().String())
	require.NoError(t, err)
	created, err := view.Watch(resource.Id)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, resource, created.GetWatch())
	pinned, err := view.Query(resource.Spec.Query)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, query, pinned.GetQuery())
	sourcePin, err := view.Source(pinned.GetQuery().Spec.Sources[0])
	require.NoError(t, err)
	sourceAdmission := native.AdmitNodeCommand{Definition: native.NodeDefinition{
		Lifetime: native.ResourceLifetime{
			NamespaceID: env.NamespaceID().String(), ResourceID: source.Id, Incarnation: sourcePin.Incarnation,
		},
		Activation: 1, Recipe: native.TemporalSourceRecipe{},
	}}
	nativeSource, err := runtime.Apply(ctx, sourceAdmission)
	require.NoError(t, err)
	require.NotNil(t, nativeSource.Node)
	require.Equal(t, native.NodeRunning, nativeSource.Node.Status)
	nativeSourceID := nativeSource.Node.NativeID
	queryAdmission := native.AdmitNodeCommand{Definition: native.NodeDefinition{
		Lifetime: native.ResourceLifetime{
			NamespaceID: env.NamespaceID().String(), ResourceID: query.Id, Incarnation: pinned.Incarnation,
		},
		Activation: 1,
		Recipe: native.QueryRecipe{
			Language: native.Cypher, Text: query.Spec.Text, Sources: []string{nativeSourceID},
		},
	}}
	nativeQuery, err := runtime.Apply(ctx, queryAdmission)
	require.NoError(t, err)
	require.NotNil(t, nativeQuery.Node)
	require.NotEqual(t, query.Id, nativeQuery.Node.NativeID)
	require.NotEqual(t, query.Revision, nativeQuery.Node.QueryRevision)
	currentQuery, err := view.Query(&watchpb.ResourceRef{Id: query.Id, Revision: query.Revision + 1})
	require.NoError(t, err)
	aliasedQuery, err := runtime.Apply(ctx, native.AdmitNodeCommand{Definition: native.NodeDefinition{
		Lifetime: native.ResourceLifetime{
			NamespaceID: env.NamespaceID().String(), ResourceID: query.Id, Incarnation: currentQuery.Incarnation,
		},
		Activation: 1,
		Recipe: native.QueryRecipe{
			Language: native.Cypher, Text: currentQuery.GetQuery().Spec.Text, Sources: []string{nativeSourceID},
		},
	}})
	require.NoError(t, err)
	require.NotNil(t, aliasedQuery.Node)
	require.Equal(t, nativeQuery.Node.NativeID, aliasedQuery.Node.NativeID)
	require.Equal(t, nativeQuery.Node.QueryRevision, aliasedQuery.Node.QueryRevision)
	metadata, err := view.NativeWatchBinding(resource.Id)
	require.NoError(t, err)
	registration := native.BindWatchCommand{Binding: metadata, QueryID: nativeQuery.Node.NativeID}
	registered, err := runtime.Apply(ctx, registration)
	require.NoError(t, err)
	require.NotNil(t, registered.Subscription)
	repeated, err := runtime.Apply(ctx, registration)
	require.NoError(t, err)
	require.Equal(t, registered.Subscription, repeated.Subscription)
	require.NoError(t, runtime.Close(ctx))
	runtime, err = native.Open(ctx, nativeConfig)
	require.NoError(t, err)
	retained, err := runtime.Apply(ctx, native.ListSubscriptionsCommand{Limit: 64})
	require.NoError(t, err)
	require.Equal(t, []native.SubscriptionObservation{*registered.Subscription}, retained.Subscriptions)
	pausedBeforeControl, err := definitions.PauseWatch(ctx, &watchpb.WatchOperationRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resource.Revision,
		RequestId: tv.RequestID() + "-pause-before-control",
	})
	require.NoError(t, err)
	resumed, err := definitions.ResumeWatch(ctx, &watchpb.WatchOperationRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: pausedBeforeControl.Revision,
		RequestId: tv.RequestID() + "-resume-before-control",
	})
	require.NoError(t, err)
	view, err = watchservice.ReadDesiredView(ctx, registry, env.NamespaceID().String(), env.Namespace().String())
	require.NoError(t, err)
	binding, err := view.RestoreNativeBinding(ctx, registry, retained.Subscriptions[0], 1)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, resource, binding.Target.Watch)
	require.NotEqual(t, resumed.Revision, binding.Target.Watch.Revision)
	executionID := binding.Target.ExecutionID
	definition, err := proto.MarshalOptions{Deterministic: true}.Marshal(binding.Target.Watch)
	require.NoError(t, err)
	_, err = control.CreateExecution(ctx, &controlpb.CreateExecutionRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: executionID, WatchId: resource.Id,
		Revision: resource.Revision, Definition: definition, RequestId: tv.RequestID() + "-execution",
	})
	require.NoError(t, err)
	controlState, err := control.MutateExecution(ctx, &controlpb.MutateExecutionRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: executionID, ExpectedRevision: resource.Revision,
		Mutation: &controlpb.MutateExecutionRequest_Claim{Claim: &controlpb.ClaimMutation{NewEpoch: 1}},
	})
	require.NoError(t, err)
	for _, paused := range []bool{true, false} {
		controlState, err = control.MutateExecution(ctx, &controlpb.MutateExecutionRequest{
			NamespaceId: env.NamespaceID().String(), ExecutionId: executionID,
			ExpectedRevision: controlState.State.Revision,
			Mutation:         &controlpb.MutateExecutionRequest_Pause{Pause: &controlpb.PauseMutation{Paused: paused}},
		})
		require.NoError(t, err)
	}
	require.Equal(t, resource.Revision, controlState.State.CreationRevision)
	require.NotEqual(t, resource.Revision, controlState.State.Revision)
	intake := &watchservice.Intake{Control: control, Workflows: env.GetTestCluster().FrontendClient()}
	poll := func() native.Event {
		var event native.Event
		await.Require(ctx, t, func(t *await.T) {
			response, err := runtime.Apply(t.Context(), native.PollWatchCommand{SubscriptionID: binding.Subscription.ID})
			require.NoError(t, err)
			require.NotNil(t, response.Event)
			event = *response.Event
		}, 10*time.Second, 20*time.Millisecond)
		return event
	}
	initial := poll()
	require.Equal(t, native.EventSnapshot, initial.Kind)
	require.Empty(t, initial.Rows)
	accepted, err := intake.ConsumeNative(ctx, runtime, binding, initial)
	require.NoError(t, err)
	require.True(t, accepted.Complete)
	_, err = runtime.Apply(ctx, native.FeedCommand{
		SourceID: nativeSourceID, Position: 1,
		Change: native.Change{
			Kind: native.Insert, ElementID: "item", Labels: []string{"Item"}, EffectiveFrom: 1,
			Properties: map[string]any{"id": "matching-item", "amount": 12},
		},
	})
	require.NoError(t, err)
	output := poll()
	require.Equal(t, native.EventDelta, output.Kind)
	require.Len(t, output.Changes, 1)
	removeFault := env.InjectResponseFault(func(request, _ any, _ error) error {
		if request, ok := request.(*controlpb.AcceptOutputRequest); ok && !request.Initial {
			return serviceerror.NewUnavailable("acceptance response lost")
		}
		return nil
	})
	_, err = intake.ConsumeNative(ctx, runtime, binding, output)
	require.ErrorContains(t, err, "acceptance response lost")
	removeFault()
	checkpoint := filepath.Join(t.TempDir(), "checkpoint")
	published, err := runtime.Apply(ctx, native.CheckpointCommand{Destination: checkpoint})
	require.NoError(t, err)
	require.NotNil(t, published.Checkpoint)
	require.Equal(t, nativeConfig.InstanceID, published.Checkpoint.InstanceID)
	require.Equal(t, nativeConfig.OwnerEpoch, published.Checkpoint.OwnerEpoch)
	require.NoError(t, runtime.Close(ctx))
	manifest, err := os.ReadFile(filepath.Join(checkpoint, "checkpoint-v1.json"))
	require.NoError(t, err)
	var actualCheckpoint native.Checkpoint
	require.NoError(t, json.Unmarshal(manifest, &actualCheckpoint))
	require.Equal(t, *published.Checkpoint, actualCheckpoint)
	digest := sha256.Sum256(manifest)
	closing := &assignmentpb.SealRequest{
		NamespaceId: opening.NamespaceId, ExecutionId: opening.ExecutionId, RequestId: tv.RequestID() + "-seal",
		ExpectedVersion: owner.Version, Grant: common.CloneProto(owner.GetActive().Grant),
		Checkpoint: &assignmentpb.Cut{
			Directory: checkpoint, ManifestSha256: hex.EncodeToString(digest[:]),
			InstanceId: actualCheckpoint.InstanceID, OwnerEpoch: actualCheckpoint.OwnerEpoch,
		},
	}
	_, err = assignments.Seal(ctx, closing)
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	sealed, err := assignments.Describe(ctx, &assignmentpb.DescribeRequest{
		NamespaceId: opening.NamespaceId, ExecutionId: opening.ExecutionId,
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, closing.Checkpoint, sealed.GetSealed().Checkpoint)
	sealRetry, err := assignments.Seal(ctx, closing)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, sealed, sealRetry)
	nextOpening.ExpectedVersion = sealed.Version
	successor, err := assignments.Begin(ctx, nextOpening)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, closing.Checkpoint, successor.GetActive().RecoveryCut)
	recovery := successor.GetActive().RecoveryCut
	retainedManifest, err := os.ReadFile(filepath.Join(recovery.Directory, "checkpoint-v1.json"))
	require.NoError(t, err)
	retainedDigest := sha256.Sum256(retainedManifest)
	require.Equal(t, recovery.ManifestSha256, hex.EncodeToString(retainedDigest[:]))
	nativeConfig.StateDir, nativeConfig.OwnerEpoch, nativeConfig.RestoreFrom =
		filepath.Join(t.TempDir(), "restored"), successor.GetActive().Grant.Epoch, recovery.Directory
	nativeConfig.StartMode = native.StartModeDormant
	runtime, err = native.Open(ctx, nativeConfig)
	require.NoError(t, err)
	dormant, err := runtime.Apply(ctx, native.ObserveNodesCommand{})
	require.NoError(t, err)
	require.Len(t, dormant.Nodes, 2)
	for _, node := range dormant.Nodes {
		require.Equal(t, native.NodeStopped, node.Status)
	}
	retained, err = runtime.Apply(ctx, native.ListSubscriptionsCommand{Limit: 64})
	require.NoError(t, err)
	require.Equal(t, []native.SubscriptionObservation{*registered.Subscription}, retained.Subscriptions)
	binding, err = view.RestoreNativeBinding(ctx, registry, retained.Subscriptions[0], 2)
	require.NoError(t, err)
	_, err = control.MutateExecution(ctx, &controlpb.MutateExecutionRequest{
		NamespaceId: env.NamespaceID().String(), ExecutionId: executionID, ExpectedRevision: controlState.State.Revision,
		Mutation: &controlpb.MutateExecutionRequest_Claim{Claim: &controlpb.ClaimMutation{PreviousEpoch: 1, NewEpoch: 2}},
	})
	require.NoError(t, err)
	restoredSource, err := runtime.Apply(ctx, sourceAdmission)
	require.NoError(t, err)
	require.Equal(t, nativeSource.Node, restoredSource.Node)
	restoredQuery, err := runtime.Apply(ctx, queryAdmission)
	require.NoError(t, err)
	require.NotNil(t, restoredQuery.Node)
	require.Equal(t, nativeQuery.Node.NativeID, restoredQuery.Node.NativeID)
	require.Equal(t, nativeQuery.Node.QueryRevision, restoredQuery.Node.QueryRevision)
	replayed := poll()
	require.Equal(t, output, replayed)
	staleBinding := binding
	staleBinding.Target.OwnerEpoch = 1
	_, err = intake.ConsumeNative(ctx, runtime, staleBinding, replayed)
	require.ErrorContains(t, err, "assignment")
	accepted, err = intake.ConsumeNative(ctx, runtime, binding, replayed)
	require.NoError(t, err)
	require.True(t, accepted.Complete)
	var received string
	require.NoError(t, env.SdkClient().GetWorkflow(ctx, resource.Spec.Action.GetStart().WorkflowId, "").Get(ctx, &received))
	require.Equal(t, "matching-item", received)
	await.Require(ctx, t, func(t *await.T) {
		response, err := control.DescribeExecution(t.Context(), &controlpb.DescribeExecutionRequest{
			NamespaceId: env.NamespaceID().String(), ExecutionId: executionID,
		})
		require.NoError(t, err)
		state := response.State
		require.Equal(t, uint64(2), state.OwnerEpoch)
		require.Equal(t, uint64(1), state.Delivered)
		require.Empty(t, state.PendingIds)
		require.True(t, state.Cursor.Complete)
		require.Equal(t, output.Sequence, state.Cursor.Sequence)
		require.Len(t, response.Deliveries, 1)
		require.Empty(t, response.Deliveries[0].Request)
	}, 10*time.Second, 100*time.Millisecond)
	restore := env.OverrideDynamicConfig(dynamicconfig.EnableDrasiWatches, false)
	defer restore()
	retried, err = definitions.CreateWatch(ctx, createWatch)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, resource, retried)
	disabledCreate := proto.Clone(createWatch).(*watchpb.CreateWatchRequest)
	disabledCreate.Id += "-disabled"
	disabledCreate.RequestId += "-disabled"
	_, err = definitions.CreateWatch(ctx, disabledCreate)
	require.ErrorContains(t, err, "disabled")
	paused, err := definitions.PauseWatch(ctx, &watchpb.WatchOperationRequest{
		Namespace: resource.Namespace, Id: resource.Id, ExpectedRevision: resumed.Revision, RequestId: tv.RequestID() + "-pause",
	})
	require.NoError(t, err)
	expectedPaused := proto.Clone(resumed).(*watchpb.Watch)
	expectedPaused.Revision++
	expectedPaused.DesiredState = watchpb.DesiredState_DESIRED_STATE_PAUSED
	protorequire.ProtoEqual(t, expectedPaused, paused)
}
