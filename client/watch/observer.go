package watch

import (
	"context"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/membership"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// MaxStatusBytes reserves room for observations in registry pages.
const MaxStatusBytes = 256

type Observer struct {
	assignments assignmentpb.AssignmentServiceClient
	membership  membership.Monitor
	rpc         common.RPCFactory
	pluginSlots chan struct{}
}

func NewObserver(assignments assignmentpb.AssignmentServiceClient, monitor membership.Monitor, rpc common.RPCFactory) *Observer {
	return &Observer{assignments: assignments, membership: monitor, rpc: rpc, pluginSlots: make(chan struct{}, 8)}
}

func (o *Observer) Observe(ctx context.Context, namespaceID string, snapshot *registrypb.ReadResponse) ([]*contractpb.ResourceStatus, error) {
	if len(snapshot.Records) == 0 {
		return nil, nil
	}
	if snapshot.Generation == 0 || len(snapshot.Records) > validation.MaxPageSize {
		return nil, serviceerror.NewDataLoss("registry returned an invalid runtime observation page")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	owner, err := o.owner(ctx, namespaceID)
	if err != nil || owner == nil {
		return nil, err
	}
	request := &watchservice.ObserveRequest{NamespaceId: namespaceID, Generation: snapshot.Generation, Owner: owner}
	for _, record := range snapshot.Records {
		target, err := RecordTarget(record)
		if err != nil {
			return nil, err
		}
		request.Targets = append(request.Targets, target)
	}
	client, err := o.queryClient(namespaceID)
	if err != nil {
		return nil, err
	}
	response, err := client.Observe(ctx, request,
		grpc.MaxCallRecvMsgSize(validation.MaxRequestBytes), grpc.MaxCallSendMsgSize(validation.MaxRequestBytes))
	if err != nil {
		return nil, err
	}
	current, err := o.owner(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(owner, current) {
		return nil, serviceerror.NewUnavailable("Watch runtime ownership changed during observation")
	}
	return observationStatuses(request, response, time.Now())
}

func (o *Observer) owner(ctx context.Context, namespaceID string) (*watchservice.Owner, error) {
	state, err := o.assignments.Describe(ctx, &assignmentpb.DescribeRequest{
		NamespaceId: namespaceID, ExecutionId: watchassignment.BusinessID,
	})
	if serviceerror.ToStatus(err).Code() == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if state.GetVersion() == 0 {
		return nil, serviceerror.NewDataLoss("runtime assignment is incomplete")
	}
	if state.GetSealed() != nil {
		return nil, nil
	}
	grant := state.GetActive().GetGrant()
	if grant.GetNamespaceId() != namespaceID || grant.GetInstanceId() != namespaceID ||
		grant.GetEpoch() == 0 || grant.GetStorageHostId() == "" || grant.GetProcessBootId() == "" {
		return nil, serviceerror.NewDataLoss("runtime assignment identity is invalid")
	}
	return &watchservice.Owner{
		Epoch: grant.Epoch, StorageHostId: grant.StorageHostId, ProcessBootId: grant.ProcessBootId,
	}, nil
}

func RecordTarget(record *registrypb.Record) (*watchservice.Target, error) {
	target := &watchservice.Target{Incarnation: record.GetIncarnation()}
	switch {
	case record.GetSource() != nil:
		target.Kind, target.Id, target.Revision = watchservice.RESOURCE_KIND_SOURCE, record.GetSource().Id, record.GetSource().Revision
	case record.GetQuery() != nil:
		target.Kind, target.Id, target.Revision = watchservice.RESOURCE_KIND_QUERY, record.GetQuery().Id, record.GetQuery().Revision
	case record.GetWatch() != nil:
		target.Kind, target.Id, target.Revision = watchservice.RESOURCE_KIND_WATCH, record.GetWatch().Id, record.GetWatch().Revision
	default:
		return nil, serviceerror.NewDataLoss("runtime target has no resource")
	}
	if target.Id == "" || target.Revision == 0 || target.Incarnation == "" {
		return nil, serviceerror.NewDataLoss("runtime target identity is incomplete")
	}
	return target, nil
}

func observationStatuses(request *watchservice.ObserveRequest, response *watchservice.ObserveResponse, now time.Time) ([]*contractpb.ResourceStatus, error) {
	if response.GetNamespaceId() != request.NamespaceId || response.GetGeneration() != request.Generation ||
		!proto.Equal(response.GetOwner(), request.Owner) || len(response.GetObservations()) != len(request.Targets) ||
		proto.Size(response) > validation.MaxRequestBytes {
		return nil, serviceerror.NewDataLoss("runtime returned an invalid observation envelope")
	}
	statuses := make([]*contractpb.ResourceStatus, len(request.Targets))
	for i, entry := range response.Observations {
		if !proto.Equal(entry.GetTarget(), request.Targets[i]) {
			return nil, serviceerror.NewDataLoss("runtime observation does not match the requested resource")
		}
		status := entry.GetStatus()
		if status == nil {
			statuses[i] = &contractpb.ResourceStatus{Message: "Runtime has not observed this resource revision"}
			continue
		}
		_, known := contractpb.LifecycleState_name[int32(status.State)]
		if !known || status.State == contractpb.LifecycleState_LIFECYCLE_STATE_UNSPECIFIED ||
			status.UpdatedAt.CheckValid() != nil || proto.Size(status) > MaxStatusBytes ||
			(status.ObservedRevision != 0 && status.ObservedRevision != request.Targets[i].Revision) ||
			status.AcceptedOutput != nil || now.Sub(status.UpdatedAt.AsTime()) > 5*time.Second ||
			status.UpdatedAt.AsTime().After(now.Add(2*time.Second)) {
			return nil, serviceerror.NewDataLoss("runtime returned invalid resource status")
		}
		statuses[i] = common.CloneProto(status)
	}
	return statuses, nil
}
