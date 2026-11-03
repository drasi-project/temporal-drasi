// Package condition waits for revision-pinned Drasi queries from Temporal workflows.
package condition

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Condition struct {
	QueryID       string
	QueryRevision uint64
	Deadline      time.Time
	// WaitTimeout bounds observation, not server acceptance. Zero uses Deadline.
	WaitTimeout time.Duration
}

type Match struct {
	ActionID string
	Before   map[string]any
	After    map[string]any
}

type CleanupStatus uint8

const (
	CleanupUnconfirmed CleanupStatus = iota
	CleanupNotNeeded
	// CleanupCancelAccepted does not retract an already accepted signal.
	CleanupCancelAccepted
)

type Result struct {
	Match Match
	// Cleanup remains meaningful when Await returns an error.
	Cleanup CleanupStatus
}

const (
	registerActivity = "drasi.condition.register.v1"
	cleanupActivity  = "drasi.condition.cleanup.v1"
)

type registration struct {
	Version uint8
	Nonce   string
	Request []byte
	Error   string
}

// Await creates one exact-run condition and waits for its signal. Repeated calls
// are separate conditions, including when their arguments are identical.
func Await(ctx workflow.Context, c Condition) (Result, error) {
	result := Result{Cleanup: CleanupNotNeeded}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	wait := c.Deadline.Sub(workflow.Now(ctx))
	if c.QueryID == "" || c.QueryRevision == 0 || wait <= 0 || c.WaitTimeout < 0 {
		return result, temporal.NewNonRetryableApplicationError("condition requires a pinned query, future deadline and nonnegative wait timeout", "InvalidArgument", nil)
	}
	if c.WaitTimeout > 0 {
		wait = min(wait, c.WaitTimeout)
	}
	info := workflow.GetInfo(ctx)
	var in registration
	if err := workflow.SideEffect(ctx, func(workflow.Context) any {
		return freezeRegistration(info.Namespace, &commonpb.WorkflowExecution{
			WorkflowId: info.WorkflowExecution.ID, RunId: info.WorkflowExecution.RunID,
		}, c, rand.Reader)
	}).Get(&in); err != nil {
		return result, err
	}
	if in.Error != "" {
		return result, temporal.NewNonRetryableApplicationError(in.Error, "DrasiConditionIdentity", nil)
	}
	req, err := decodeRegistration(in)
	if err != nil {
		return result, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidArgument", nil)
	}
	signals := workflow.GetSignalChannel(ctx, req.Spec.Action.GetSignal().SignalName)
	options := workflow.GetActivityOptions(ctx)
	options.ActivityID = "drasi-condition-register-" + in.Nonce
	options.StartToCloseTimeout = cmp.Or(options.StartToCloseTimeout, 10*time.Second)
	options.ScheduleToCloseTimeout = cmp.Or(options.ScheduleToCloseTimeout, time.Minute)
	options.WaitForCancellation = false
	registerCtx, stopRegistration := workflow.WithCancel(workflow.WithActivityOptions(ctx, options))
	defer stopRegistration()
	pending := workflow.ExecuteActivity(registerCtx, registerActivity, in)
	result.Cleanup = CleanupUnconfirmed

	timerCtx, stopTimer := workflow.WithCancel(ctx)
	defer stopTimer()
	timer := workflow.NewTimer(timerCtx, wait)
	var primary error
	finished := false
	received := false
	receive := func(channel workflow.ReceiveChannel) {
		var raw json.RawMessage
		channel.Receive(ctx, &raw)
		result.Match, primary = decodeMatch(raw, req.Spec.Query.Revision)
		finished, received = true, true
	}
	selector := workflow.NewSelector(ctx)
	selector.AddReceive(signals, func(channel workflow.ReceiveChannel, _ bool) { receive(channel) })
	selector.AddFuture(pending, func(f workflow.Future) {
		if err := f.Get(ctx, nil); err != nil {
			primary, finished = err, true
		}
	})
	selector.AddFuture(timer, func(workflow.Future) {
		primary = temporal.NewNonRetryableApplicationError(
			"condition wait elapsed; acceptance may have occurred", "DrasiConditionWaitTimeout", nil)
		finished = true
	})
	selector.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {
		primary, finished = ctx.Err(), true
	})
	for !finished {
		selector.Select(ctx)
	}
	// An observed signal wins over a concurrently ready local stop.
	if primary != nil && !received {
		var raw json.RawMessage
		if signals.ReceiveAsync(&raw) {
			result.Match, primary = decodeMatch(raw, req.Spec.Query.Revision)
		}
	}
	if primary == nil {
		result.Cleanup = CleanupNotNeeded
		return result, nil
	}

	stopRegistration()
	cleanupCtx, stopCleanup := workflow.NewDisconnectedContext(ctx)
	defer stopCleanup()
	options.ActivityID = "drasi-condition-cleanup-" + in.Nonce
	options.StartToCloseTimeout = 10 * time.Second
	options.ScheduleToCloseTimeout = 30 * time.Second
	cleanupCtx = workflow.WithActivityOptions(cleanupCtx, options)
	if err := workflow.ExecuteActivity(cleanupCtx, cleanupActivity, in).Get(cleanupCtx, nil); err != nil {
		workflow.GetLogger(ctx).Error("Drasi condition cleanup is unconfirmed", "watchId", req.Id, "error", err)
		if temporal.IsCanceledError(primary) {
			return result, temporal.NewCanceledError("Drasi condition cleanup is unconfirmed", err.Error())
		}
		return result, temporal.NewNonRetryableApplicationError(
			"Drasi condition cleanup is unconfirmed: "+err.Error(), "DrasiConditionCleanupUnconfirmed", primary)
	}
	result.Cleanup = CleanupCancelAccepted
	return result, primary
}

func freezeRegistration(namespace string, owner *commonpb.WorkflowExecution, c Condition, entropy io.Reader) registration {
	in := registration{Version: 1}
	var nonce [32]byte
	if _, err := io.ReadFull(entropy, nonce[:]); err != nil {
		in.Error = "condition identity entropy failed: " + err.Error()
		return in
	}
	in.Nonce = hex.EncodeToString(nonce[:])
	req := &watchpb.CreateWatchRequest{
		Namespace: namespace, Id: "condition." + in.Nonce, RequestId: "condition.create." + in.Nonce,
		Spec: &watchpb.WatchSpec{
			Query: &watchpb.ResourceRef{Id: c.QueryID, Revision: c.QueryRevision}, Trigger: watchpb.Trigger_TRIGGER_ENTER,
			Lifetime: &watchpb.WatchSpec_Once{Once: &watchpb.Once{Owner: owner, Deadline: timestamppb.New(c.Deadline)}},
			Action: &watchpb.WorkflowAction{Operation: &watchpb.WorkflowAction_Signal{Signal: &watchpb.WorkflowSignal{
				Target:     &watchpb.WorkflowSignal_Run{Run: proto.Clone(owner).(*commonpb.WorkflowExecution)},
				SignalName: "drasi.condition." + in.Nonce,
			}}},
		},
	}
	if err := validation.ValidateRequest(req); err != nil {
		in.Error = err.Error()
		return in
	}
	var err error
	in.Request, err = (proto.MarshalOptions{Deterministic: true}).Marshal(req)
	if err != nil {
		in.Error = err.Error()
	}
	return in
}

func decodeRegistration(in registration) (*watchpb.CreateWatchRequest, error) {
	nonce, err := hex.DecodeString(in.Nonce)
	if in.Version != 1 || in.Error != "" || err != nil || len(nonce) != 32 {
		return nil, errors.New("invalid condition registration marker")
	}
	var req watchpb.CreateWatchRequest
	if err := proto.Unmarshal(in.Request, &req); err != nil {
		return nil, fmt.Errorf("decode condition registration: %w", err)
	}
	if err := validation.ValidateRequest(&req); err != nil {
		return nil, err
	}
	if req.Id != "condition."+in.Nonce || req.RequestId != "condition.create."+in.Nonce ||
		req.Spec.GetOnce() == nil || req.Spec.Action.GetSignal().GetSignalName() != "drasi.condition."+in.Nonce ||
		req.Spec.Action.ResultPayload != watchpb.ResultPayload_RESULT_PAYLOAD_APPEND {
		return nil, errors.New("condition registration identity does not match its marker")
	}
	return &req, nil
}

func decodeMatch(raw json.RawMessage, revision uint64) (Match, error) {
	var change watchpb.ResultChange
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &change); err != nil {
		return Match{}, temporal.NewNonRetryableApplicationError("invalid condition signal", "DrasiConditionProtocol", err)
	}
	output := change.GetOutput()
	if change.Trigger != watchpb.Trigger_TRIGGER_ENTER || change.ActionId == "" || change.After == nil ||
		output.GetQueryRevision() != revision || output.GetIncarnation() == "" || output.GetRowIdentity() == "" {
		return Match{}, temporal.NewNonRetryableApplicationError("condition signal does not identify a matching ENTER result", "DrasiConditionProtocol", nil)
	}
	match := Match{ActionID: change.ActionId, After: change.After.AsMap()}
	if change.Before != nil {
		match.Before = change.Before.AsMap()
	}
	return match, nil
}
