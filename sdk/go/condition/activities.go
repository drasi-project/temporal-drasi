package condition

import (
	"context"
	"errors"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type activities struct {
	client watchpb.WatchServiceClient
}

// RegisterActivities borrows the application's authenticated Watch client.
func RegisterActivities(reg worker.ActivityRegistry, client watchpb.WatchServiceClient) error {
	if reg == nil || client == nil {
		return errors.New("condition activities require a registry and authenticated Watch client")
	}
	a := &activities{client: client}
	reg.RegisterActivityWithOptions(a.register, activity.RegisterOptions{Name: registerActivity})
	reg.RegisterActivityWithOptions(a.cleanup, activity.RegisterOptions{Name: cleanupActivity})
	return nil
}

func (a *activities) register(ctx context.Context, in registration) error {
	req, err := activityRequest(ctx, in, "register")
	if err != nil {
		return err
	}
	_, err = a.create(ctx, req)
	return err
}

func (a *activities) cleanup(ctx context.Context, in registration) error {
	req, err := activityRequest(ctx, in, "cleanup")
	if err != nil {
		return err
	}
	// Recover the original receipt before cancellation, including after a lost response.
	created, err := a.create(ctx, req)
	if err != nil {
		return err
	}
	canceled, err := a.client.CancelWatch(ctx, &watchpb.WatchOperationRequest{
		Namespace: req.Namespace, Id: req.Id, ExpectedRevision: created.Revision,
		RequestId: "condition.cancel." + in.Nonce,
	})
	if err != nil {
		return rpcError(err)
	}
	if canceled.GetNamespace() != req.Namespace || canceled.GetId() != req.Id || canceled.GetRevision() <= created.Revision {
		return temporal.NewNonRetryableApplicationError("invalid condition cancellation receipt", "DrasiConditionProtocol", nil)
	}
	return nil
}

func (a *activities) create(ctx context.Context, req *watchpb.CreateWatchRequest) (*watchpb.Watch, error) {
	created, err := a.client.CreateWatch(ctx, req)
	if err != nil {
		return nil, rpcError(err)
	}
	if created.GetNamespace() != req.Namespace || created.GetId() != req.Id || created.GetRevision() == 0 ||
		!proto.Equal(created.GetSpec(), req.Spec) {
		return nil, temporal.NewNonRetryableApplicationError("invalid condition registration receipt", "DrasiConditionProtocol", nil)
	}
	return created, nil
}

func activityRequest(ctx context.Context, in registration, phase string) (*watchpb.CreateWatchRequest, error) {
	req, err := decodeRegistration(in)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), "InvalidArgument", nil)
	}
	info := activity.GetInfo(ctx)
	owner := req.Spec.GetOnce().Owner
	if info.IsLocalActivity || info.Namespace != req.Namespace || info.WorkflowExecution.ID != owner.WorkflowId ||
		info.WorkflowExecution.RunID != owner.RunId || info.ActivityID != "drasi-condition-"+phase+"-"+in.Nonce {
		return nil, temporal.NewNonRetryableApplicationError("condition registration does not belong to this activity's exact workflow run", "FailedPrecondition", nil)
	}
	return req, nil
}

func rpcError(err error) error {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied,
		codes.Unauthenticated, codes.FailedPrecondition, codes.Unimplemented:
		return temporal.NewNonRetryableApplicationError(err.Error(), status.Code(err).String(), err)
	default:
		return err
	}
}
