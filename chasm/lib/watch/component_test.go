package watch

import (
	"context"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type testEnvironment struct {
	ctx       context.Context
	engine    *chasmtest.Engine
	ref       chasm.ComponentRef
	component *Component
	client    *workflowservicemock.MockWorkflowServiceClient
	dispatch  *DispatchHandler
	clock     *clock.EventTimeSource
	create    Create
}

func newTestEnvironment(t *testing.T, configure ...func(*Create)) *testEnvironment {
	t.Helper()
	env := newUninitializedTestEnvironment(t, configure...)
	_, err := env.accept(Batch{Revision: env.create.Revision, Incarnation: env.create.Incarnation, Initial: true, Complete: true})
	require.NoError(t, err)
	return env
}

func newUninitializedTestEnvironment(t *testing.T, configure ...func(*Create)) *testEnvironment {
	t.Helper()
	tv := testvars.New(t)
	ts := clock.NewEventTimeSource()
	ts.Update(time.Unix(1_000, 0).UTC())
	client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	dispatch := &DispatchHandler{Client: client, CallTimeout: time.Second}
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&Library{Dispatch: dispatch, Deadline: &DeadlineHandler{}}))
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts),
		chasmtest.WithInvariantCheck(func(t *testing.T, node *chasm.Node, root chasm.RootComponent) {
			reloaded, err := chasm.NewTreeFromDB(node.Snapshot(nil).Nodes, registry, &chasm.MockNodeBackend{},
				chasm.DefaultPathEncoder, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly), metrics.NoopMetricsHandler)
			require.NoError(t, err)
			ctx := chasm.NewContext(t.Context(), reloaded)
			component, err := reloaded.Component(ctx, chasm.ComponentRef{})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, root.(*Component).State, component.(*Component).State)
			require.Len(t, component.(*Component).Intents, len(root.(*Component).Intents))
		}),
	)
	ctx := chasm.NewEngineContext(t.Context(), engine)
	input := Create{
		Namespace:   tv.NamespaceName().String(),
		ID:          tv.WorkflowID(),
		Revision:    1,
		Incarnation: "query-incarnation",
		Limits:      Limits{Pending: 2, Batch: 2, Recent: 1, PendingBytes: 16_384},
	}
	var err error
	input.Definition, err = proto.Marshal(&contractpb.Watch{
		Namespace: input.Namespace, Id: input.ID, Revision: input.Revision,
		Spec: &contractpb.WatchSpec{Query: &contractpb.ResourceRef{Id: "query", Revision: 3}},
	})
	require.NoError(t, err)
	for _, configure := range configure {
		configure(&input)
	}
	var component *Component
	key := chasm.ExecutionKey{NamespaceID: tv.NamespaceID().String(), BusinessID: input.ID}
	_, err = chasm.StartExecution(ctx, key, func(ctx chasm.MutableContext, input Create) (*Component, error) {
		var err error
		component, err = New(ctx, input)
		return component, err
	}, input)
	require.NoError(t, err)
	return &testEnvironment{
		ctx: ctx, engine: engine, ref: chasm.NewComponentRef[*Component](key),
		component: component, client: client, dispatch: dispatch, clock: ts, create: input,
	}
}

func (c *Component) retry(ctx chasm.MutableContext, id string, acknowledgeAmbiguous bool) error {
	intent, err := c.retryIntent(ctx, id, acknowledgeAmbiguous)
	if err != nil {
		return err
	}
	c.authorizeRetry(ctx, intent, acknowledgeAmbiguous)
	return nil
}

func TestInitialSnapshotSequence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		sequence uint64
	}{
		{"empty query baseline", 0},
		{"query already evaluated before watch", 17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newUninitializedTestEnvironment(t)
			batch := testBatch()
			batch.Initial, batch.Sequence = true, tc.sequence
			receipt, err := env.accept(batch)
			require.NoError(t, err)
			require.Len(t, receipt.ActionIDs, 1)
			protorequire.ProtoEqual(t, &watchpb.Cursor{
				Sequence: tc.sequence, NextOrdinal: 1, Complete: true,
			}, receipt.Cursor)
			next := testBatch()
			next.Sequence = tc.sequence + 1
			_, err = env.accept(next)
			require.NoError(t, err)
		})
	}
}

func TestSnapshotRequiresCompleteContiguousBoundary(t *testing.T) {
	t.Parallel()
	env := newUninitializedTestEnvironment(t, func(input *Create) { input.Incarnation = "" })
	_, err := env.accept(testBatch())
	require.ErrorContains(t, err, "initial query snapshot")
	snapshot := Batch{
		Revision: 1, Incarnation: "query-incarnation", Sequence: 17,
		ItemCount: 1, Initial: true,
	}
	_, err = env.accept(snapshot)
	require.NoError(t, err)
	state := env.state(t)
	require.Equal(t, "query-incarnation", state.Incarnation)
	require.Equal(t, watchpb.PROGRESS_PHASE_SNAPSHOTTING, state.ProgressPhase)
	delta := testBatch()
	delta.Sequence = 18
	_, err = env.accept(delta)
	require.ErrorContains(t, err, "snapshot chunk gap")
	protorequire.ProtoEqual(t, state, env.state(t))
	snapshot.FirstOrdinal, snapshot.Complete = 1, true
	_, err = env.accept(snapshot)
	require.NoError(t, err)
	require.Equal(t, watchpb.PROGRESS_PHASE_LIVE, env.state(t).ProgressPhase)
	_, err = env.accept(delta)
	require.NoError(t, err)
	snapshot.FirstOrdinal, snapshot.ItemCount = 0, 2
	receipt, err := env.accept(snapshot)
	require.NoError(t, err)
	require.True(t, receipt.Duplicate)
	snapshot.Sequence = 19
	_, err = env.accept(snapshot)
	require.ErrorContains(t, err, "explicit reset")
}

func testBatch() Batch {
	return Batch{
		Revision: 1, Incarnation: "query-incarnation", Sequence: 1,
		ItemCount: 1, Complete: true,
		Actions: []Action{{
			RowKey: "row",
			Request: &workflowservice.StartWorkflowExecutionRequest{
				WorkflowId:            "target-workflow",
				WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
				WorkflowType: &commonpb.WorkflowType{
					Name: "test-workflow-type",
				},
				TaskQueue: &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			},
		}},
	}
}

func (e *testEnvironment) accept(batch Batch) (*Receipt, error) {
	result, _, err := chasm.UpdateComponent(
		e.ctx, e.ref,
		func(c *Component, ctx chasm.MutableContext, batch Batch) (*Receipt, error) {
			return c.Accept(ctx, batch)
		},
		batch,
	)
	return result, err
}

func (e *testEnvironment) update(update func(*Component, chasm.MutableContext) error) error {
	_, _, err := chasm.UpdateComponent(
		e.ctx, e.ref,
		func(c *Component, ctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			return nil, update(c, ctx)
		},
		nil,
	)
	return err
}

func (e *testEnvironment) state(t *testing.T) *watchpb.State {
	t.Helper()
	state, err := chasm.ReadComponent(
		e.ctx, e.ref,
		func(c *Component, _ chasm.Context, _ chasm.NoValue) (*watchpb.State, error) {
			return common.CloneProto(c.State), nil
		},
		nil,
	)
	require.NoError(t, err)
	return state
}

func (e *testEnvironment) intent(t *testing.T, id string) *watchpb.Intent {
	t.Helper()
	intent, err := chasm.ReadComponent(
		e.ctx, e.ref,
		func(c *Component, ctx chasm.Context, _ chasm.NoValue) (*watchpb.Intent, error) {
			return common.CloneProto(c.Intents[id].Get(ctx)), nil
		},
		nil,
	)
	require.NoError(t, err)
	return intent
}

func TestAcceptanceAndOwnershipFence(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	receipt, err := env.accept(batch)
	require.NoError(t, err)
	require.Len(t, receipt.ActionIDs, 1)
	id := receipt.ActionIDs[0]
	original := batch.Actions[0].Request.(*workflowservice.StartWorkflowExecutionRequest)
	require.Empty(t, original.RequestId)
	request := &workflowservice.StartWorkflowExecutionRequest{}
	require.NoError(t, proto.Unmarshal(env.intent(t, id).Request, request))
	expected := common.CloneProto(original)
	expected.Namespace, expected.RequestId = env.create.Namespace, id
	protorequire.ProtoEqual(t, expected, request)
	protorequire.ProtoEqual(t, &watchpb.Cursor{Sequence: 1, NextOrdinal: 1, Complete: true}, receipt.Cursor)

	require.NoError(t, env.update(func(c *Component, _ chasm.MutableContext) error {
		return c.Claim(1, 0, 1)
	}))
	_, err = env.accept(batch)
	require.ErrorContains(t, err, "runtime assignment")
	batch.OwnerEpoch = 1
	duplicate, err := env.accept(batch)
	require.NoError(t, err)
	require.Equal(t, &Receipt{Cursor: receipt.Cursor, Duplicate: true}, duplicate)
	require.Equal(t, []string{id}, env.state(t).PendingIds)
}

func TestAcceptanceRejectsWithoutAdvancing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Batch)
		error  string
	}{
		{"stale revision", func(b *Batch) { b.Revision++ }, "definition"},
		{"stale owner", func(b *Batch) { b.OwnerEpoch++ }, "assignment"},
		{"different incarnation", func(b *Batch) { b.Incarnation = "different" }, "incarnation"},
		{"sequence gap", func(b *Batch) { b.Sequence = 2 }, "gap requires recovery"},
		{"ordinal gap", func(b *Batch) { b.FirstOrdinal = 1 }, "gap requires recovery"},
		{"zero sequence", func(b *Batch) { b.Sequence = 0 }, "invalid query output"},
		{"missing row identity", func(b *Batch) { b.Actions[0].RowKey = "" }, "identify a row"},
		{"outside chunk", func(b *Batch) { b.Actions[0].Ordinal = 1 }, "identify a row"},
		{"invalid action", func(b *Batch) { b.Actions[0].Request = &commonpb.Payload{} }, "unsupported"},
		{"unresolved signal", func(b *Batch) {
			b.Actions[0].Request = &workflowservice.SignalWorkflowExecutionRequest{
				SignalName: "signal", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "target"},
			}
		}, "resolved workflow run"},
		{"destructive workflow reuse", func(b *Batch) {
			//nolint:staticcheck // The deprecated destructive policy must still be rejected.
			b.Actions[0].Request.(*workflowservice.StartWorkflowExecutionRequest).WorkflowIdReusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
		}, "could terminate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newTestEnvironment(t)
			before := env.state(t)
			batch := testBatch()
			tc.change(&batch)
			_, err := env.accept(batch)
			require.ErrorContains(t, err, tc.error)
			protorequire.ProtoEqual(t, before, env.state(t))
		})
	}
}

func TestLargeOutputChunksAndBackpressure(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	batch := testBatch()
	batch.Complete = false
	first, err := env.accept(batch)
	require.NoError(t, err)
	batch.FirstOrdinal, batch.Actions[0].Ordinal = 1, 1
	second, err := env.accept(batch)
	require.NoError(t, err)
	before := env.state(t)
	batch.FirstOrdinal, batch.Actions[0].Ordinal = 2, 2
	batch.Complete = true
	_, err = env.accept(batch)
	require.ErrorContains(t, err, "action buffer is full")
	protorequire.ProtoEqual(t, before, env.state(t))
	require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
		return c.Complete(ctx, first.ActionIDs[0], 0, "target-run", "")
	}))
	last, err := env.accept(batch)
	require.NoError(t, err)
	require.Equal(t, []string{second.ActionIDs[0], last.ActionIDs[0]}, env.state(t).PendingIds)
	protorequire.ProtoEqual(t, &watchpb.Cursor{Sequence: 1, NextOrdinal: 3, Complete: true}, last.Cursor)
}

func TestActionByteLimit(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t, func(c *Create) { c.Limits.PendingBytes = 1 })
	before := env.state(t)
	_, err := env.accept(testBatch())
	require.ErrorContains(t, err, "byte buffer is full")
	protorequire.ProtoEqual(t, before, env.state(t))
}

func TestPauseAndCancelDrainAcceptedActions(t *testing.T) {
	t.Parallel()
	for _, cancel := range []bool{false, true} {
		name := "pause"
		if cancel {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newTestEnvironment(t)
			receipt, err := env.accept(testBatch())
			require.NoError(t, err)
			require.NoError(t, env.update(func(c *Component, _ chasm.MutableContext) error {
				if cancel {
					return c.Cancel(1)
				}
				return c.Pause(1, true)
			}))
			next := testBatch()
			next.Revision = env.state(t).Revision
			next.Sequence = 2
			_, err = env.accept(next)
			require.ErrorContains(t, err, "not accepting")
			env.client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
				Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "target-run"}, nil)
			dropped, err := chasmtest.ExecuteSideEffectTask(
				env.ctx, env.engine, env.component, env.dispatch, chasm.TaskAttributes{},
				&watchpb.DispatchTask{ActionId: receipt.ActionIDs[0]},
			)
			require.NoError(t, err)
			require.False(t, dropped)
			require.Empty(t, env.state(t).PendingIds)
			require.Equal(t, uint64(1), env.state(t).Delivered)
		})
	}
}

func TestResumePreservesActionDefinitionIdentity(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	require.NoError(t, env.update(func(c *Component, _ chasm.MutableContext) error {
		return c.Pause(1, true)
	}))
	require.NoError(t, env.update(func(c *Component, _ chasm.MutableContext) error {
		return c.Pause(2, false)
	}))
	batch := testBatch()
	batch.Revision = 3
	receipt, err := env.accept(batch)
	require.NoError(t, err)
	id := ActionID(ActionIdentity{
		Namespace: env.create.Namespace, WatchID: env.create.ID, Revision: env.create.Revision,
		Incarnation: batch.Incarnation, Sequence: batch.Sequence,
		Ordinal: batch.Actions[0].Ordinal, RowKey: batch.Actions[0].RowKey,
	})
	require.Equal(t, []string{id}, receipt.ActionIDs)
	require.Equal(t, env.create.Definition, env.state(t).Definition)
	var request workflowservice.StartWorkflowExecutionRequest
	require.NoError(t, proto.Unmarshal(env.intent(t, id).Request, &request))
	require.Equal(t, id, request.RequestId)
	batch.Revision = 1
	_, err = env.accept(batch)
	require.ErrorContains(t, err, "definition")
}

func TestOneShotAndDeadline(t *testing.T) {
	t.Parallel()
	t.Run("expired before asynchronous control creation", func(t *testing.T) {
		t.Parallel()
		env := newUninitializedTestEnvironment(t, func(c *Create) {
			c.Once, c.Deadline = true, time.Unix(999, 0).UTC()
		})
		state := env.state(t)
		require.Equal(t, watchpb.STATUS_TIMED_OUT, state.Status)
		require.Empty(t, state.PendingIds)
		_, err := env.accept(testBatch())
		require.ErrorContains(t, err, "accepting")
		require.Empty(t, env.state(t).PendingIds)
	})
	t.Run("first match", func(t *testing.T) {
		t.Parallel()
		env := newTestEnvironment(t, func(c *Create) { c.Once = true })
		batch := testBatch()
		batch.ItemCount = 2
		batch.Actions = append(batch.Actions, Action{
			Ordinal: 1, RowKey: "second-row", Request: batch.Actions[0].Request,
		})
		receipt, err := env.accept(batch)
		require.NoError(t, err)
		require.Len(t, receipt.ActionIDs, 1)
		require.Equal(t, watchpb.STATUS_SATISFIED, env.state(t).Status)
	})
	t.Run("deadline while paused", func(t *testing.T) {
		t.Parallel()
		deadline := time.Unix(1_010, 0).UTC()
		env := newTestEnvironment(t, func(c *Create) { c.Once, c.Deadline = true, deadline })
		require.NoError(t, env.update(func(c *Component, _ chasm.MutableContext) error {
			return c.Pause(1, true)
		}))
		env.clock.Update(deadline)
		fired, err := env.engine.FirePureTasks(env.ref, deadline)
		require.NoError(t, err)
		require.Equal(t, 1, fired)
		require.Equal(t, watchpb.STATUS_TIMED_OUT, env.state(t).Status)
	})
}

func TestCompletedDeliveryRetention(t *testing.T) {
	t.Parallel()
	env := newTestEnvironment(t)
	var previous string
	for sequence := uint64(1); sequence <= 4; sequence++ {
		batch := testBatch()
		batch.Sequence = sequence
		receipt, err := env.accept(batch)
		require.NoError(t, err)
		id := receipt.ActionIDs[0]
		require.NoError(t, env.update(func(c *Component, ctx chasm.MutableContext) error {
			return c.Complete(ctx, id, 0, "target-run", "")
		}))
		require.Equal(t, []string{id}, env.state(t).RecentIds)
		require.Empty(t, env.intent(t, id).Request)
		if previous != "" {
			retained, err := chasm.ReadComponent(
				env.ctx, env.ref,
				func(c *Component, _ chasm.Context, _ chasm.NoValue) (bool, error) {
					_, ok := c.Intents[previous]
					return ok, nil
				}, nil,
			)
			require.NoError(t, err)
			require.False(t, retained)
		}
		previous = id
	}
	require.Zero(t, env.state(t).PendingBytes)
	require.Equal(t, uint64(4), env.state(t).Delivered)
}
