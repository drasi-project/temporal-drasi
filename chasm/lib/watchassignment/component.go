package watchassignment

import (
	"bytes"
	"crypto/sha256"
	"math"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

const BusinessID = "drasi-namespace-assignment-v1"

type Component struct {
	chasm.UnimplementedComponent
	*assignmentpb.State
}

var _ chasm.RootComponent = (*Component)(nil)

func (*Component) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (*Component) ContextMetadata(chasm.Context) map[string]string { return nil }

func (*Component) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("namespace assignment retains owner epochs and cannot be terminated")
}

type beginInput struct {
	request         *assignmentpb.BeginRequest
	acceptanceError string
}

func (c *Component) begin(_ chasm.MutableContext, input beginInput) (*assignmentpb.State, error) {
	request := input.request
	fingerprint, err := fingerprint(request)
	if err != nil {
		return nil, err
	}
	var predecessor *assignmentpb.Active
	var cut *assignmentpb.Cut
	switch phase := c.Phase.(type) {
	case *assignmentpb.State_Active:
		predecessor = phase.Active
	case *assignmentpb.State_Sealed:
		predecessor, cut = phase.Sealed.Predecessor, phase.Sealed.Checkpoint
	case nil:
		if c.Version != 0 {
			return nil, serviceerror.NewFailedPrecondition("assignment state has no compatible ownership phase")
		}
	default:
		return nil, serviceerror.NewFailedPrecondition("assignment state has no compatible ownership phase")
	}
	if predecessor != nil && request.RequestId == predecessor.RequestId {
		if !bytes.Equal(fingerprint, predecessor.Fingerprint) {
			return nil, serviceerror.NewInvalidArgument("request_id was used with different Begin content")
		}
		if c.GetActive() == nil {
			return nil, serviceerror.NewFailedPrecondition("assignment grant has been sealed")
		}
		return common.CloneProto(c.State), nil
	}
	if c.GetActive() != nil {
		return nil, serviceerror.NewFailedPrecondition("assignment already has an active owner")
	}
	if input.acceptanceError != "" {
		return nil, serviceerror.NewFailedPrecondition(input.acceptanceError)
	}
	if request.ExpectedVersion != c.Version {
		return nil, serviceerror.NewFailedPrecondition("assignment version does not match")
	}
	epoch := uint64(1)
	if predecessor != nil {
		if request.StorageHostId != predecessor.Grant.StorageHostId {
			return nil, serviceerror.NewFailedPrecondition("assignment recovery requires the same storage host")
		}
		if predecessor.Grant.Epoch == math.MaxUint64 {
			return nil, serviceerror.NewFailedPrecondition("assignment epoch exhausted")
		}
		epoch = predecessor.Grant.Epoch + 1
	}
	if c.Version > math.MaxUint64-2 {
		return nil, serviceerror.NewFailedPrecondition("assignment version exhausted")
	}
	c.State = &assignmentpb.State{Version: c.Version + 1, Phase: &assignmentpb.State_Active{
		Active: &assignmentpb.Active{
			Grant: &assignmentpb.Grant{
				NamespaceId: request.NamespaceId, InstanceId: request.NamespaceId,
				StorageHostId: request.StorageHostId, ProcessBootId: request.ProcessBootId, Epoch: epoch,
			},
			RequestId: request.RequestId, Fingerprint: fingerprint, RecoveryCut: common.CloneProto(cut),
		},
	}}
	return common.CloneProto(c.State), nil
}

func (c *Component) seal(_ chasm.MutableContext, request *assignmentpb.SealRequest) (*assignmentpb.State, error) {
	fingerprint, err := fingerprint(request)
	if err != nil {
		return nil, err
	}
	if sealed := c.GetSealed(); sealed != nil {
		if request.RequestId == sealed.RequestId {
			if !bytes.Equal(fingerprint, sealed.Fingerprint) {
				return nil, serviceerror.NewInvalidArgument("request_id was used with different Seal content")
			}
			return common.CloneProto(c.State), nil
		}
		return nil, serviceerror.NewFailedPrecondition("assignment is already sealed")
	}
	active := c.GetActive()
	if active == nil || !proto.Equal(request.Grant, active.Grant) {
		return nil, serviceerror.NewFailedPrecondition("assignment grant does not match the active owner")
	}
	if request.RequestId == active.RequestId {
		return nil, serviceerror.NewInvalidArgument("Seal requires a new request_id")
	}
	if request.ExpectedVersion != c.Version {
		return nil, serviceerror.NewFailedPrecondition("assignment version does not match")
	}
	if c.Version == math.MaxUint64 {
		return nil, serviceerror.NewFailedPrecondition("assignment version exhausted")
	}
	c.State = &assignmentpb.State{Version: c.Version + 1, Phase: &assignmentpb.State_Sealed{
		Sealed: &assignmentpb.Sealed{
			Predecessor: common.CloneProto(active), RequestId: request.RequestId, Fingerprint: fingerprint,
			Checkpoint: common.CloneProto(request.Checkpoint),
		},
	}}
	return common.CloneProto(c.State), nil
}

func (c *Component) describe(chasm.Context, chasm.NoValue) (*assignmentpb.State, error) {
	return common.CloneProto(c.State), nil
}

func fingerprint(request proto.Message) ([]byte, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	return digest[:], nil
}
