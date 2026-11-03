package condition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type watchClient struct {
	watchpb.WatchServiceClient
	create func(context.Context, *watchpb.CreateWatchRequest) (*watchpb.Watch, error)
	cancel func(context.Context, *watchpb.WatchOperationRequest) (*watchpb.Watch, error)
}

func TestAwaitIsolation(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	requests := make(chan *watchpb.CreateWatchRequest, 8)
	client := &watchClient{create: func(_ context.Context, req *watchpb.CreateWatchRequest) (*watchpb.Watch, error) {
		requests <- proto.Clone(req).(*watchpb.CreateWatchRequest)
		for range 2 {
			if err := signalMatch(env, req, req.Id); err != nil {
				return nil, err
			}
		}
		return createdWatch(req), nil
	}}
	require.NoError(t, RegisterActivities(env, client))
	env.ExecuteWorkflow(func(ctx workflow.Context) ([]Result, error) {
		var results []Result
		for range 2 {
			futures := make([]workflow.Future, 2)
			for i := range futures {
				future, promise := workflow.NewFuture(ctx)
				futures[i] = future
				workflow.Go(ctx, func(ctx workflow.Context) {
					r, err := Await(ctx, Condition{QueryID: "existing-query", QueryRevision: 7, Deadline: workflow.Now(ctx).Add(time.Minute)})
					promise.Set(r, err)
				})
			}
			for _, future := range futures {
				var r Result
				if err := future.Get(ctx, &r); err != nil {
					return nil, err
				}
				results = append(results, r)
			}
		}
		return results, nil
	})
	require.NoError(t, env.GetWorkflowError())
	var results []Result
	require.NoError(t, env.GetWorkflowResult(&results))
	require.Len(t, results, 4)
	ids := make(map[string]struct{})
	signals := make(map[string]struct{})
	for _, result := range results {
		require.Equal(t, map[string]any{"query": "existing-query"}, result.Match.After)
		ids[result.Match.ActionID] = struct{}{}
	}
	for range 4 {
		select {
		case req := <-requests:
			_, ok := ids[req.Id]
			require.True(t, ok)
			signals[req.Spec.Action.GetSignal().SignalName] = struct{}{}
		default:
			t.Fatal("an isolated wait did not register")
		}
	}
	require.Len(t, ids, 4)
	require.Len(t, signals, 4)
}

func TestAwaitRetryUsesFrozenRequest(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	requests := make(chan *watchpb.CreateWatchRequest, 8)
	client := &watchClient{create: func(_ context.Context, req *watchpb.CreateWatchRequest) (*watchpb.Watch, error) {
		requests <- proto.Clone(req).(*watchpb.CreateWatchRequest)
		if len(requests) == 1 {
			return nil, status.Error(codes.Unavailable, "accepted response was lost")
		}
		if err := signalMatch(env, req, "accepted"); err != nil {
			return nil, err
		}
		return createdWatch(req), nil
	}}
	require.NoError(t, RegisterActivities(env, client))
	env.ExecuteWorkflow(func(ctx workflow.Context) (Result, error) {
		return Await(ctx, Condition{QueryID: "existing-query", QueryRevision: 7, Deadline: workflow.Now(ctx).Add(time.Minute)})
	})
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, requests, 2)
	first, second := <-requests, <-requests
	require.True(t, proto.Equal(first, second))
}

type waitOutcome struct {
	Result       Result
	Error        string
	ErrorType    string
	Canceled     bool
	CleanupError string
}

func TestAwaitCleanup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		cancelWait  bool
		confirmed   bool
		deadline    time.Duration
		createError error
		cancelError error
		wantStatus  CleanupStatus
		wantType    string
	}{
		{name: "lost-response-timeout", deadline: time.Hour, wantStatus: CleanupCancelAccepted, wantType: "DrasiConditionWaitTimeout"},
		{name: "canceled", cancelWait: true, deadline: time.Hour, wantStatus: CleanupCancelAccepted},
		{name: "canceled-after-registration", cancelWait: true, confirmed: true, deadline: time.Hour, wantStatus: CleanupCancelAccepted},
		{name: "expired-without-receipt", deadline: time.Second, createError: status.Error(codes.InvalidArgument, "deadline passed"), wantStatus: CleanupUnconfirmed, wantType: "DrasiConditionCleanupUnconfirmed"},
		{name: "revision-conflict", deadline: time.Hour, cancelError: status.Error(codes.FailedPrecondition, "revision changed"), wantStatus: CleanupUnconfirmed, wantType: "DrasiConditionCleanupUnconfirmed"},
		{name: "cleanup-exhausted", deadline: time.Hour, cancelError: status.Error(codes.Unavailable, "cancel response lost"), wantStatus: CleanupUnconfirmed, wantType: "DrasiConditionCleanupUnconfirmed"},
		{name: "canceled-cleanup-rejected", cancelWait: true, deadline: time.Hour, cancelError: status.Error(codes.FailedPrecondition, "revision changed"), wantStatus: CleanupUnconfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			creates := make(chan *watchpb.CreateWatchRequest, 16)
			cancels := make(chan *watchpb.WatchOperationRequest, 16)
			client := &watchClient{
				create: func(_ context.Context, req *watchpb.CreateWatchRequest) (*watchpb.Watch, error) {
					creates <- proto.Clone(req).(*watchpb.CreateWatchRequest)
					if len(creates) == 1 && !tc.confirmed {
						return nil, status.Error(codes.Unavailable, "accepted response was lost")
					}
					if tc.createError != nil {
						return nil, tc.createError
					}
					return createdWatch(req), nil
				},
				cancel: func(_ context.Context, req *watchpb.WatchOperationRequest) (*watchpb.Watch, error) {
					cancels <- proto.Clone(req).(*watchpb.WatchOperationRequest)
					if tc.cancelError != nil {
						return nil, tc.cancelError
					}
					return &watchpb.Watch{Namespace: req.Namespace, Id: req.Id, Revision: req.ExpectedRevision + 1}, nil
				},
			}
			require.NoError(t, RegisterActivities(env, client))
			env.ExecuteWorkflow(func(ctx workflow.Context) (waitOutcome, error) {
				ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					RetryPolicy: &temporal.RetryPolicy{InitialInterval: 10 * time.Second},
				})
				waitCtx, cancel := workflow.WithCancel(ctx)
				defer cancel()
				waitTimeout := time.Second
				var cancellation workflow.Future
				if tc.cancelWait {
					waitTimeout = time.Minute
					future, promise := workflow.NewFuture(ctx)
					cancellation = future
					workflow.Go(ctx, func(ctx workflow.Context) {
						err := workflow.NewTimer(ctx, time.Second).Get(ctx, nil)
						if err == nil {
							cancel()
						}
						promise.Set(nil, err)
					})
				}
				result, err := Await(waitCtx, Condition{
					QueryID: "existing-query", QueryRevision: 7,
					Deadline: workflow.Now(ctx).Add(tc.deadline), WaitTimeout: waitTimeout,
				})
				if cancellation != nil {
					if err := cancellation.Get(ctx, nil); err != nil {
						return waitOutcome{}, err
					}
				}
				out := waitOutcome{Result: result, Canceled: temporal.IsCanceledError(err)}
				if err != nil {
					out.Error = err.Error()
				}
				if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
					out.ErrorType = app.Type()
				}
				if canceled, ok := errors.AsType[*temporal.CanceledError](err); ok && canceled.HasDetails() {
					var message, cause string
					if err := canceled.Details(&message, &cause); err != nil {
						return out, err
					}
					out.CleanupError = message + ": " + cause
				}
				return out, nil
			})
			require.NoError(t, env.GetWorkflowError())
			var out waitOutcome
			require.NoError(t, env.GetWorkflowResult(&out))
			require.Equal(t, tc.wantStatus, out.Result.Cleanup)
			require.Equal(t, tc.cancelWait, out.Canceled)
			require.Equal(t, tc.wantType, out.ErrorType)
			require.NotEmpty(t, out.Error)
			require.GreaterOrEqual(t, len(creates), 2)
			first := <-creates
			for len(creates) > 0 {
				require.True(t, proto.Equal(first, <-creates), "cleanup must recover the identical create receipt")
			}
			if tc.createError != nil {
				require.Empty(t, cancels)
			} else {
				require.NotEmpty(t, cancels)
				cancel := <-cancels
				require.True(t, proto.Equal(&watchpb.WatchOperationRequest{
					Namespace: first.Namespace, Id: first.Id, ExpectedRevision: 1,
					RequestId: "condition.cancel." + first.Id[len("condition."):],
				}, cancel))
				for len(cancels) > 0 {
					require.True(t, proto.Equal(cancel, <-cancels))
				}
			}
			if tc.wantStatus == CleanupUnconfirmed {
				if tc.cancelWait {
					require.Contains(t, out.CleanupError, "cleanup is unconfirmed")
				} else {
					require.Contains(t, out.Error, "cleanup is unconfirmed")
				}
			}
		})
	}
}

func TestSignalValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"malformed", `{"trigger":`},
		{"missing-output", `{"actionId":"action","trigger":"TRIGGER_ENTER","after":{}}`},
		{"wrong-revision", `{"actionId":"action","trigger":"TRIGGER_ENTER","output":{"queryRevision":"8","incarnation":"i","rowIdentity":"r"},"after":{}}`},
		{"not-enter", `{"actionId":"action","trigger":"TRIGGER_LEAVE","output":{"queryRevision":"7","incarnation":"i","rowIdentity":"r"},"after":{}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			match, err := decodeMatch(json.RawMessage(tc.raw), 7)
			require.ErrorContains(t, err, "condition signal")
			require.Equal(t, Match{}, match)
		})
	}
}

func TestRegistrationEntropyAndEncoding(t *testing.T) {
	t.Parallel()
	owner := &commonpb.WorkflowExecution{WorkflowId: "owner", RunId: "run"}
	c := Condition{QueryID: "query", QueryRevision: 7, Deadline: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	failed := freezeRegistration("namespace", owner, c, bytes.NewReader(nil))
	require.Contains(t, failed.Error, io.EOF.Error())
	require.Empty(t, failed.Request)
	require.Empty(t, failed.Nonce)
	in := freezeRegistration("namespace", owner, c, bytes.NewReader(make([]byte, 32)))
	require.Empty(t, in.Error)
	req, err := decodeRegistration(in)
	require.NoError(t, err)
	require.Equal(t, owner, req.Spec.GetOnce().Owner)
	require.Equal(t, owner, req.Spec.Action.GetSignal().GetRun())
	require.Equal(t, c.Deadline, req.Spec.GetOnce().Deadline.AsTime())
	in.Version = 2
	_, err = decodeRegistration(in)
	require.ErrorContains(t, err, "marker")
}

func TestActivityOwnerFence(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"namespace", "workflow", "run", "activity"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			calls := make(chan struct{}, 1)
			client := &watchClient{create: func(_ context.Context, req *watchpb.CreateWatchRequest) (*watchpb.Watch, error) {
				calls <- struct{}{}
				return createdWatch(req), nil
			}}
			require.NoError(t, RegisterActivities(env, client))
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				info := workflow.GetInfo(ctx)
				namespace := info.Namespace
				owner := &commonpb.WorkflowExecution{WorkflowId: info.WorkflowExecution.ID, RunId: info.WorkflowExecution.RunID}
				switch field {
				case "namespace":
					namespace = "other-namespace"
				case "workflow":
					owner.WorkflowId = "other-workflow"
				case "run":
					owner.RunId = "retired-run"
				default:
				}
				in := freezeRegistration(namespace, owner, Condition{
					QueryID: "existing-query", QueryRevision: 7, Deadline: workflow.Now(ctx).Add(time.Minute),
				}, bytes.NewReader(make([]byte, 32)))
				id := "drasi-condition-register-" + in.Nonce
				if field == "activity" {
					id = "different-activity"
				}
				ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ActivityID: id, StartToCloseTimeout: time.Second})
				return workflow.ExecuteActivity(ctx, registerActivity, in).Get(ctx, nil)
			})
			app, ok := errors.AsType[*temporal.ApplicationError](env.GetWorkflowError())
			require.True(t, ok)
			require.Equal(t, "FailedPrecondition", app.Type())
			require.Empty(t, calls)
		})
	}
}

func TestMalformedSignalDoesNotConsumeFollowingMatch(t *testing.T) {
	t.Parallel()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	client := &watchClient{
		create: func(_ context.Context, req *watchpb.CreateWatchRequest) (*watchpb.Watch, error) {
			env.SignalWorkflow(req.Spec.Action.GetSignal().SignalName, json.RawMessage(`{"actionId":"invalid"}`))
			if err := signalMatch(env, req, "valid-after-invalid"); err != nil {
				return nil, err
			}
			return createdWatch(req), nil
		},
		cancel: func(_ context.Context, req *watchpb.WatchOperationRequest) (*watchpb.Watch, error) {
			return &watchpb.Watch{Namespace: req.Namespace, Id: req.Id, Revision: req.ExpectedRevision + 1}, nil
		},
	}
	require.NoError(t, RegisterActivities(env, client))
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		_, err := Await(ctx, Condition{QueryID: "existing-query", QueryRevision: 7, Deadline: workflow.Now(ctx).Add(time.Minute)})
		return err
	})
	app, ok := errors.AsType[*temporal.ApplicationError](env.GetWorkflowError())
	require.True(t, ok)
	require.Equal(t, "DrasiConditionProtocol", app.Type())
}

func (c *watchClient) CreateWatch(ctx context.Context, req *watchpb.CreateWatchRequest, _ ...grpc.CallOption) (*watchpb.Watch, error) {
	return c.create(ctx, req)
}

func (c *watchClient) CancelWatch(ctx context.Context, req *watchpb.WatchOperationRequest, _ ...grpc.CallOption) (*watchpb.Watch, error) {
	return c.cancel(ctx, req)
}

func TestAwaitEarlySignal(t *testing.T) {
	t.Parallel()
	for _, lostResponse := range []bool{false, true} {
		name := "confirmed"
		if lostResponse {
			name = "lost-response"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			requests := make(chan *watchpb.CreateWatchRequest, 8)
			client := &watchClient{create: func(_ context.Context, req *watchpb.CreateWatchRequest) (*watchpb.Watch, error) {
				requests <- proto.Clone(req).(*watchpb.CreateWatchRequest)
				if err := signalMatch(env, req, "accepted"); err != nil {
					return nil, err
				}
				if lostResponse {
					return nil, status.Error(codes.Unavailable, "accepted response was lost")
				}
				return createdWatch(req), nil
			}}
			require.NoError(t, RegisterActivities(env, client))
			env.ExecuteWorkflow(func(ctx workflow.Context) (Result, error) {
				return Await(ctx, Condition{QueryID: "existing-query", QueryRevision: 7, Deadline: workflow.Now(ctx).Add(time.Minute)})
			})
			require.NoError(t, env.GetWorkflowError())
			var result Result
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, Result{Match: Match{ActionID: "accepted", After: map[string]any{"query": "existing-query"}}, Cleanup: CleanupNotNeeded}, result)
			select {
			case req := <-requests:
				require.Equal(t, req.Spec.GetOnce().Owner, req.Spec.Action.GetSignal().GetRun())
				require.NotEmpty(t, req.Spec.GetOnce().Owner.RunId)
			default:
				t.Fatal("registration did not reach the public client")
			}
		})
	}
}

func createdWatch(req *watchpb.CreateWatchRequest) *watchpb.Watch {
	return &watchpb.Watch{Namespace: req.Namespace, Id: req.Id, Revision: 1, Spec: proto.Clone(req.Spec).(*watchpb.WatchSpec)}
}

func signalMatch(env *testsuite.TestWorkflowEnvironment, req *watchpb.CreateWatchRequest, actionID string) error {
	row, err := structpb.NewStruct(map[string]any{"query": req.Spec.Query.Id})
	if err != nil {
		return err
	}
	raw, err := protojson.Marshal(&watchpb.ResultChange{
		ActionId: actionID, Trigger: watchpb.Trigger_TRIGGER_ENTER, After: row,
		Output: &watchpb.OutputIdentity{QueryRevision: req.Spec.Query.Revision, Incarnation: "incarnation", Sequence: 1, RowIdentity: "row"},
	})
	if err != nil {
		return err
	}
	env.SignalWorkflow(req.Spec.Action.GetSignal().SignalName, json.RawMessage(raw))
	return nil
}
