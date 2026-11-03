package watchregistry

import (
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"google.golang.org/protobuf/proto"
)

type publicMutation interface {
	proto.Message
	GetNamespace() string
	GetId() string
	GetRequestId() string
}

type operation int

const (
	create operation = iota
	update
	remove
	start
	stop
	pause
	cancel
	reset
	retryDelivery
)

type mutation struct {
	public publicMutation
	kind   registrypb.ResourceKind
	action operation
}

func (m mutation) accepts() bool {
	return m.action != remove && m.action != stop && m.action != pause && m.action != cancel && m.action != retryDelivery
}

func decodeMutation(request *registrypb.MutateRequest) (mutation, error) {
	var m mutation
	switch op := request.Operation.(type) {
	case *registrypb.MutateRequest_CreateSource:
		m = mutation{op.CreateSource, registrypb.RESOURCE_KIND_SOURCE, create}
	case *registrypb.MutateRequest_UpdateSource:
		m = mutation{op.UpdateSource, registrypb.RESOURCE_KIND_SOURCE, update}
	case *registrypb.MutateRequest_DeleteSource:
		m = mutation{op.DeleteSource, registrypb.RESOURCE_KIND_SOURCE, remove}
	case *registrypb.MutateRequest_StartSource:
		m = mutation{op.StartSource, registrypb.RESOURCE_KIND_SOURCE, start}
	case *registrypb.MutateRequest_StopSource:
		m = mutation{op.StopSource, registrypb.RESOURCE_KIND_SOURCE, stop}
	case *registrypb.MutateRequest_CreateQuery:
		m = mutation{op.CreateQuery, registrypb.RESOURCE_KIND_QUERY, create}
	case *registrypb.MutateRequest_UpdateQuery:
		m = mutation{op.UpdateQuery, registrypb.RESOURCE_KIND_QUERY, update}
	case *registrypb.MutateRequest_DeleteQuery:
		m = mutation{op.DeleteQuery, registrypb.RESOURCE_KIND_QUERY, remove}
	case *registrypb.MutateRequest_StartQuery:
		m = mutation{op.StartQuery, registrypb.RESOURCE_KIND_QUERY, start}
	case *registrypb.MutateRequest_StopQuery:
		m = mutation{op.StopQuery, registrypb.RESOURCE_KIND_QUERY, stop}
	case *registrypb.MutateRequest_CreateWatch:
		m = mutation{op.CreateWatch, registrypb.RESOURCE_KIND_WATCH, create}
	case *registrypb.MutateRequest_UpdateWatch:
		m = mutation{op.UpdateWatch, registrypb.RESOURCE_KIND_WATCH, update}
	case *registrypb.MutateRequest_DeleteWatch:
		m = mutation{op.DeleteWatch, registrypb.RESOURCE_KIND_WATCH, remove}
	case *registrypb.MutateRequest_PauseWatch:
		m = mutation{op.PauseWatch, registrypb.RESOURCE_KIND_WATCH, pause}
	case *registrypb.MutateRequest_ResumeWatch:
		m = mutation{op.ResumeWatch, registrypb.RESOURCE_KIND_WATCH, start}
	case *registrypb.MutateRequest_CancelWatch:
		m = mutation{op.CancelWatch, registrypb.RESOURCE_KIND_WATCH, cancel}
	case *registrypb.MutateRequest_ResetWatch:
		m = mutation{op.ResetWatch, registrypb.RESOURCE_KIND_WATCH, reset}
	case *registrypb.MutateRequest_RetryDelivery:
		m = mutation{op.RetryDelivery, registrypb.RESOURCE_KIND_WATCH, retryDelivery}
	default:
		return mutation{}, serviceerror.NewInvalidArgument("registry mutation operation is required")
	}
	if !m.public.ProtoReflect().IsValid() {
		return mutation{}, serviceerror.NewInvalidArgument("registry mutation request is required")
	}
	return m, nil
}
