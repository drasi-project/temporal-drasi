package watch

import (
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newHandlerTestEnvironment(
	t *testing.T,
	configure ...func(*contractpb.Watch),
) (*testEnvironment, *handler, *watchpb.CreateExecutionRequest) {
	t.Helper()
	tv := testvars.New(t)
	env := newUninitializedTestEnvironment(t, func(input *Create) {
		input.Namespace = strings.ReplaceAll(input.Namespace, "/", "-")
		input.ID = strings.ReplaceAll(input.ID, "/", "-")
		resource := &contractpb.Watch{
			Namespace: input.Namespace, Id: input.ID, Revision: input.Revision,
			Spec: &contractpb.WatchSpec{
				Query:    &contractpb.ResourceRef{Id: "query", Revision: 1},
				Trigger:  contractpb.Trigger_TRIGGER_ENTER,
				Lifetime: &contractpb.WatchSpec_Persistent{Persistent: &emptypb.Empty{}},
				Action: &contractpb.WorkflowAction{Operation: &contractpb.WorkflowAction_Start{
					Start: &contractpb.WorkflowStart{
						WorkflowId: tv.WithWorkflowIDNumber(1).WorkflowID(), WorkflowType: tv.WorkflowType().Name,
						TaskQueue: tv.TaskQueue().Name, ReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
					},
				}},
			},
		}
		for _, configure := range configure {
			configure(resource)
		}
		var err error
		input.Definition, err = proto.Marshal(resource)
		require.NoError(t, err)
		input.Incarnation = ""
		if once := resource.Spec.GetOnce(); once != nil {
			input.Once, input.Deadline = true, once.Deadline.AsTime()
		}
	})
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: env.ref.NamespaceID, Name: env.create.Namespace}, nil, "active")
	namespaces := namespace.NewMockRegistry(gomock.NewController(t))
	namespaces.EXPECT().GetNamespaceByID(namespace.ID(env.ref.NamespaceID)).Return(ns, nil).AnyTimes()
	request := &watchpb.CreateExecutionRequest{
		NamespaceId: env.ref.NamespaceID, ExecutionId: env.create.ID, WatchId: env.create.ID,
		Revision: env.create.Revision, Definition: env.create.Definition, RequestId: tv.RequestID(),
		Once: env.create.Once,
	}
	if env.create.Once {
		request.Deadline = timestamppb.New(env.create.Deadline)
	}
	return env, &handler{
		namespaces: namespaces, enabled: func(string) bool { return true }, limits: env.create.Limits,
	}, request
}

func TestControlRequestValidation(t *testing.T) {
	t.Parallel()
	tv := testvars.New(t)
	for _, tc := range []struct {
		name    string
		request keyedRequest
		message string
	}{
		{"missing request", nil, "missing or exceeds"},
		{"typed nil request", (*watchpb.CreateExecutionRequest)(nil), "missing or exceeds"},
		{"missing namespace", &watchpb.DescribeExecutionRequest{ExecutionId: tv.WorkflowID()}, "namespace UUID"},
		{"invalid namespace", &watchpb.DescribeExecutionRequest{NamespaceId: "not-a-uuid", ExecutionId: tv.WorkflowID()}, "namespace UUID"},
		{"missing execution", &watchpb.DescribeExecutionRequest{NamespaceId: tv.NamespaceID().String()}, "execution ID"},
		{"oversize execution", &watchpb.DescribeExecutionRequest{NamespaceId: tv.NamespaceID().String(), ExecutionId: strings.Repeat("x", 513)}, "execution ID"},
		{"oversize request", &watchpb.CreateExecutionRequest{Definition: make([]byte, validation.MaxRequestBytes)}, "exceeds the size limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := &handler{}
			_, _, err := h.resolve(tc.request)
			require.ErrorContains(t, err, tc.message)
		})
	}
}

func TestControlNamespaceEnablement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		enabled bool
		global  bool
		message string
	}{
		{"disabled", false, false, "disabled"},
		{"global namespace", true, true, "global namespaces"},
		{"enabled local namespace", true, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ns := namespace.NewNamespaceForTest(nil, nil, tc.global, nil, 0)
			h := &handler{enabled: func(string) bool { return tc.enabled }}
			err := h.checkEnabled(ns)
			if tc.message == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.message)
			}
		})
	}
}

func TestControlCreateDefinitionValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		change  func(*watchpb.CreateExecutionRequest)
		message string
	}{
		{"malformed definition", func(request *watchpb.CreateExecutionRequest) { request.Definition = []byte{0xff} }, "malformed"},
		{"zero revision", func(request *watchpb.CreateExecutionRequest) { request.Revision = 0 }, "execution identity"},
		{"mismatched watch", func(request *watchpb.CreateExecutionRequest) { request.WatchId += "-other" }, "execution identity"},
		{"missing request ID", func(request *watchpb.CreateExecutionRequest) { request.RequestId = "" }, "request_id"},
		{"mismatched lifetime", func(request *watchpb.CreateExecutionRequest) { request.Once = true }, "lifetime"},
		{"unexpected deadline", func(request *watchpb.CreateExecutionRequest) { request.Deadline = timestamppb.New(time.Unix(2_000, 0)) }, "lifetime"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env, h, request := newHandlerTestEnvironment(t)
			tc.change(request)
			_, err := h.CreateExecution(env.ctx, request)
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, uint64(1), env.state(t).Revision)
		})
	}
}

func TestControlCreationRetryAfterLifecycleChange(t *testing.T) {
	t.Parallel()
	env, h, request := newHandlerTestEnvironment(t)
	_, err := h.MutateExecution(env.ctx, &watchpb.MutateExecutionRequest{
		NamespaceId: request.NamespaceId, ExecutionId: request.ExecutionId, ExpectedRevision: 1,
		Mutation: &watchpb.MutateExecutionRequest_Pause{Pause: &watchpb.PauseMutation{Paused: true}},
	})
	require.NoError(t, err)
	response, err := h.CreateExecution(env.ctx, request)
	require.NoError(t, err)
	require.Equal(t, uint64(2), response.State.Revision)
	require.Equal(t, uint64(1), response.State.CreationRevision)
	require.Equal(t, watchpb.STATUS_PAUSED, response.State.Status)
	var resource contractpb.Watch
	require.NoError(t, proto.Unmarshal(request.Definition, &resource))
	resource.Spec.Action.GetStart().TaskQueue += "-changed"
	request.Definition, err = proto.Marshal(&resource)
	require.NoError(t, err)
	_, err = h.CreateExecution(env.ctx, request)
	require.ErrorContains(t, err, "bound to another definition")
}

func TestControlCreationRetryAcceptsEquivalentDefinitionEncoding(t *testing.T) {
	t.Parallel()
	env, h, request := newHandlerTestEnvironment(t)
	var original contractpb.Watch
	require.NoError(t, proto.Unmarshal(request.Definition, &original))
	request.Definition = protowire.AppendString(
		protowire.AppendTag(request.Definition, 1, protowire.BytesType), original.Namespace)
	var retry contractpb.Watch
	require.NoError(t, proto.Unmarshal(request.Definition, &retry))
	protorequire.ProtoEqual(t, &original, &retry)
	response, err := h.CreateExecution(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, env.state(t), response.State)
}

func TestControlCreationRetryIgnoresResourceLifecycleMetadata(t *testing.T) {
	t.Parallel()
	env, h, request := newHandlerTestEnvironment(t)
	var resource contractpb.Watch
	require.NoError(t, proto.Unmarshal(request.Definition, &resource))
	resource.DesiredState = contractpb.DesiredState_DESIRED_STATE_RUNNING
	resource.Status = &contractpb.ResourceStatus{State: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING}
	var err error
	request.Definition, err = proto.Marshal(&resource)
	require.NoError(t, err)
	response, err := h.CreateExecution(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, env.state(t), response.State)
}

func TestControlCreationRetryAfterDeadline(t *testing.T) {
	t.Parallel()
	env, h, request := newHandlerTestEnvironment(t, func(resource *contractpb.Watch) {
		tv := testvars.New(t).WithRunID("00000000-0000-4000-8000-000000000001")
		owner := &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: tv.RunID()}
		resource.Spec.Lifetime = &contractpb.WatchSpec_Once{Once: &contractpb.Once{
			Owner: owner, Deadline: timestamppb.New(time.Unix(2_000, 0)),
		}}
		resource.Spec.Action.Operation = &contractpb.WorkflowAction_Signal{Signal: &contractpb.WorkflowSignal{
			Target: &contractpb.WorkflowSignal_Run{Run: owner}, SignalName: tv.SignalName(),
		}}
	})
	env.clock.Update(time.Unix(3_000, 0))
	response, err := h.CreateExecution(env.ctx, request)
	require.NoError(t, err)
	require.Equal(t, request.Revision, response.State.CreationRevision)
}

func TestControlAcceptAndRedactedDescription(t *testing.T) {
	t.Parallel()
	env, h, create := newHandlerTestEnvironment(t)
	_, err := h.MutateExecution(env.ctx, &watchpb.MutateExecutionRequest{
		NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId, ExpectedRevision: 1,
		Mutation: &watchpb.MutateExecutionRequest_Claim{Claim: &watchpb.ClaimMutation{NewEpoch: 1}},
	})
	require.NoError(t, err)
	action := testBatch().Actions[0]
	request := &watchpb.AcceptOutputRequest{
		NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId,
		Revision: 1, OwnerEpoch: 1, Incarnation: "query-incarnation", Sequence: 0,
		Initial: true, Complete: true, ItemCount: 1,
		Actions: []*watchpb.PreparedAction{{
			RowIdentity: action.RowKey,
			Operation:   &watchpb.PreparedAction_Start{Start: action.Request.(*workflowservice.StartWorkflowExecutionRequest)},
		}},
	}
	response, err := h.AcceptOutput(env.ctx, request)
	require.NoError(t, err)
	require.Len(t, response.ActionIds, 1)
	repeated, err := h.AcceptOutput(env.ctx, request)
	require.NoError(t, err)
	require.True(t, repeated.Duplicate)
	protorequire.ProtoEqual(t, response.Cursor, repeated.Cursor)
	description, err := h.DescribeExecution(env.ctx, &watchpb.DescribeExecutionRequest{
		NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId,
	})
	require.NoError(t, err)
	require.Len(t, description.Deliveries, 1)
	require.Empty(t, description.Deliveries[0].Request)
	description.State.Revision = 99
	require.Equal(t, uint64(1), env.state(t).Revision)
	require.NotEmpty(t, env.intent(t, response.ActionIds[0]).Request)
}

func TestControlRejectsStaleAndMissingMutations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		request *watchpb.MutateExecutionRequest
		message string
	}{
		{"missing revision", &watchpb.MutateExecutionRequest{}, "expected revision"},
		{"stale revision", &watchpb.MutateExecutionRequest{ExpectedRevision: 2}, "revision changed"},
		{"missing mutation", &watchpb.MutateExecutionRequest{ExpectedRevision: 1}, "mutation is missing"},
		{"empty claim", &watchpb.MutateExecutionRequest{ExpectedRevision: 1, Mutation: &watchpb.MutateExecutionRequest_Claim{}}, "assignment is missing"},
		{"empty pause", &watchpb.MutateExecutionRequest{ExpectedRevision: 1, Mutation: &watchpb.MutateExecutionRequest_Pause{}}, "pause attributes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env, h, create := newHandlerTestEnvironment(t)
			request := tc.request
			request.NamespaceId, request.ExecutionId = create.NamespaceId, create.ExecutionId
			before := env.state(t)
			_, err := h.MutateExecution(env.ctx, request)
			require.ErrorContains(t, err, tc.message)
			protorequire.ProtoEqual(t, before, env.state(t))
		})
	}
}

func TestControlRuntimeMutationFencesDelayedPreviousOwner(t *testing.T) {
	t.Parallel()
	for _, mutation := range []string{"pause", "cancel"} {
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()
			env, h, create := newHandlerTestEnvironment(t)
			for previous := uint64(0); previous < 2; previous++ {
				_, err := h.MutateExecution(env.ctx, &watchpb.MutateExecutionRequest{
					NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId,
					ExpectedRevision: 1, OwnerEpoch: previous,
					Mutation: &watchpb.MutateExecutionRequest_Claim{Claim: &watchpb.ClaimMutation{
						PreviousEpoch: previous, NewEpoch: previous + 1,
					}},
				})
				require.NoError(t, err)
			}
			before := env.state(t)
			request := &watchpb.MutateExecutionRequest{
				NamespaceId: create.NamespaceId, ExecutionId: create.ExecutionId, ExpectedRevision: 1, OwnerEpoch: 1,
			}
			if mutation == "pause" {
				request.Mutation = &watchpb.MutateExecutionRequest_Pause{Pause: &watchpb.PauseMutation{Paused: true}}
			} else {
				request.Mutation = &watchpb.MutateExecutionRequest_Cancel{Cancel: &emptypb.Empty{}}
			}
			_, err := h.MutateExecution(env.ctx, request)
			require.ErrorContains(t, err, "runtime assignment changed")
			protorequire.ProtoEqual(t, before, env.state(t))
			request.OwnerEpoch = 0
			_, err = h.MutateExecution(env.ctx, request)
			require.NoError(t, err)
			require.Equal(t, uint64(2), env.state(t).Revision)
		})
	}
}
