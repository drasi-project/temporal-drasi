package watch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type (
	DispatchHandler struct {
		chasm.SideEffectTaskHandlerBase[*watchpb.DispatchTask]
		Client      workflowservice.WorkflowServiceClient
		CallTimeout time.Duration
	}

	DeadlineHandler struct {
		chasm.PureTaskHandlerBase
	}
)

func (*DispatchHandler) Validate(
	ctx chasm.Context,
	component *Component,
	_ chasm.TaskInvocation,
	task *watchpb.DispatchTask,
) (bool, error) {
	if len(component.PendingIds) == 0 || component.PendingIds[0] != task.ActionId {
		return false, nil
	}
	field, ok := component.Intents[task.ActionId]
	if !ok {
		return false, nil
	}
	return field.Get(ctx).Status == watchpb.DELIVERY_STATUS_PENDING, nil
}

func (h *DispatchHandler) Execute(
	ctx context.Context,
	ref chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *watchpb.DispatchTask,
) error {
	intent, _, err := chasm.UpdateComponent(
		ctx, ref,
		func(c *Component, ctx chasm.MutableContext, _ chasm.NoValue) (*watchpb.Intent, error) {
			return c.BeginDispatch(ctx, task.ActionId)
		},
		nil,
	)
	if err != nil || intent == nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, h.CallTimeout)
	defer cancel()
	runID, deliveryErr := h.dispatch(callCtx, intent)
	code := serviceerror.ToStatus(deliveryErr).Code()
	if deliveryErr != nil && !terminalDeliveryCode(code) {
		if !intent.SafeRetry {
			return h.recordAmbiguous(ctx, ref, task.ActionId, intent.Attempts)
		}
		return deliveryErr
	}
	if deliveryErr != nil && intent.Uncertain &&
		(code == codes.NotFound || code == codes.AlreadyExists) {
		return h.recordAmbiguous(ctx, ref, task.ActionId, intent.Attempts)
	}
	terminalError := ""
	if deliveryErr != nil {
		// Service error text can contain workflow input and must not enter watch status.
		terminalError = code.String()
	}
	_, _, err = chasm.UpdateComponent(
		ctx, ref,
		func(c *Component, ctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			return nil, c.Complete(ctx, task.ActionId, intent.Attempts, runID, terminalError)
		},
		nil,
	)
	return err
}

func (*DispatchHandler) recordAmbiguous(ctx context.Context, ref chasm.ComponentRef, id string, attempt uint32) error {
	_, _, err := chasm.UpdateComponent(
		ctx, ref,
		func(c *Component, ctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
			c.Ambiguous(ctx, id, attempt)
			return nil, nil
		}, nil,
	)
	return err
}

func (h *DispatchHandler) dispatch(ctx context.Context, intent *watchpb.Intent) (string, error) {
	switch intent.Kind {
	case watchpb.ACTION_KIND_START:
		request := &workflowservice.StartWorkflowExecutionRequest{}
		if err := proto.Unmarshal(intent.Request, request); err != nil {
			return "", queueerrors.NewUnprocessableTaskError(fmt.Sprintf("decode watch start intent: %v", err))
		}
		response, err := h.Client.StartWorkflowExecution(ctx, request)
		if already, ok := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err); ok &&
			already.StartRequestId == intent.ActionId {
			return already.RunId, nil
		}
		return response.GetRunId(), err
	case watchpb.ACTION_KIND_SIGNAL:
		request := &workflowservice.SignalWorkflowExecutionRequest{}
		if err := proto.Unmarshal(intent.Request, request); err != nil {
			return "", queueerrors.NewUnprocessableTaskError(fmt.Sprintf("decode watch signal intent: %v", err))
		}
		_, err := h.Client.SignalWorkflowExecution(ctx, request)
		return request.GetWorkflowExecution().GetRunId(), err
	case watchpb.ACTION_KIND_SIGNAL_WITH_START:
		request := &workflowservice.SignalWithStartWorkflowExecutionRequest{}
		if err := proto.Unmarshal(intent.Request, request); err != nil {
			return "", queueerrors.NewUnprocessableTaskError(fmt.Sprintf("decode watch signal-with-start intent: %v", err))
		}
		response, err := h.Client.SignalWithStartWorkflowExecution(ctx, request)
		return response.GetRunId(), err
	default:
		return "", queueerrors.NewUnprocessableTaskError("unknown watch action kind")
	}
}

func terminalDeliveryCode(code codes.Code) bool {
	switch code {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
		codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition,
		codes.Unimplemented:
		return true
	default:
		return false
	}
}

func (*DeadlineHandler) Validate(
	_ chasm.Context,
	component *Component,
	_ chasm.TaskInvocation,
	task *watchpb.DeadlineTask,
) (bool, error) {
	return component.Once && component.DeadlineUnixNano == task.DeadlineUnixNano &&
		(component.Status == watchpb.STATUS_RUNNING || component.Status == watchpb.STATUS_PAUSED), nil
}

func (*DeadlineHandler) Execute(
	ctx chasm.MutableContext,
	component *Component,
	_ chasm.TaskAttributes,
	task *watchpb.DeadlineTask,
) error {
	if ctx.Now(component).Before(time.Unix(0, task.DeadlineUnixNano)) {
		return serviceerror.NewInternal("watch deadline task executed before its deadline")
	}
	component.Status = watchpb.STATUS_TIMED_OUT
	return nil
}
