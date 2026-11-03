package watch

import (
	"context"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Service) Observe(ctx context.Context, request *watchservice.ObserveRequest) (*watchservice.ObserveResponse, error) {
	if request == nil || proto.Size(request) > validation.MaxRequestBytes || request.Generation == 0 ||
		request.GetOwner().GetEpoch() == 0 || len(request.Targets) == 0 || len(request.Targets) > validation.MaxPageSize {
		return nil, serviceerror.NewInvalidArgument("runtime observation requires a bounded registry cut and owner")
	}
	id, err := uuid.Parse(request.NamespaceId)
	if err != nil || id == uuid.Nil || id.String() != request.NamespaceId {
		return nil, serviceerror.NewInvalidArgument("runtime observation requires a canonical namespace UUID")
	}
	ns, err := s.params.Namespaces.GetNamespaceByID(namespace.ID(request.NamespaceId))
	if err != nil {
		return nil, err
	}
	if ns.IsGlobalNamespace() {
		return nil, serviceerror.NewFailedPrecondition("Watch runtime does not support global namespaces")
	}
	if err := validateObservationTargets(request, ns.Name().String()); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	hosted := s.namespaces[ns.ID()]
	if hosted == nil || hosted.graph == nil || hosted.closed || hosted.problem != "" {
		s.mu.Unlock()
		return nil, serviceerror.NewUnavailable("Watch namespace runtime is not observable")
	}
	graph := hosted.graph
	s.mu.Unlock()
	entries, err := graph.observeStatus(request, time.Now())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	valid := s.namespaces[ns.ID()] == hosted && hosted.graph == graph && !hosted.closed && hosted.problem == ""
	s.mu.Unlock()
	if !valid {
		return nil, serviceerror.NewUnavailable("Watch namespace runtime changed during observation")
	}
	return &watchservice.ObserveResponse{
		NamespaceId: request.NamespaceId, Generation: request.Generation,
		Owner: common.CloneProto(request.Owner), Observations: entries,
	}, nil
}

func validateObservationTargets(request *watchservice.ObserveRequest, namespaceName string) error {
	seen := make(map[resourceKey]struct{}, len(request.Targets))
	for _, target := range request.Targets {
		kind := observationKind(target.GetKind())
		if kind == registrypb.RESOURCE_KIND_UNSPECIFIED || target.GetRevision() == 0 ||
			target.GetIncarnation() == "" || len(target.Incarnation) > 120 {
			return serviceerror.NewInvalidArgument("runtime observation target is incomplete")
		}
		if err := validation.ValidateRequest(&contractpb.GetSourceRequest{Namespace: namespaceName, Id: target.Id}); err != nil {
			return err
		}
		key := resourceKey{kind, target.Id}
		if _, ok := seen[key]; ok {
			return serviceerror.NewInvalidArgument("runtime observation contains duplicate resources")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func observationKind(kind watchservice.ResourceKind) registrypb.ResourceKind {
	switch kind {
	case watchservice.RESOURCE_KIND_SOURCE:
		return registrypb.RESOURCE_KIND_SOURCE
	case watchservice.RESOURCE_KIND_QUERY:
		return registrypb.RESOURCE_KIND_QUERY
	case watchservice.RESOURCE_KIND_WATCH:
		return registrypb.RESOURCE_KIND_WATCH
	default:
		return registrypb.RESOURCE_KIND_UNSPECIFIED
	}
}

func (g *Graph) observeStatus(request *watchservice.ObserveRequest, now time.Time) ([]*watchservice.ResourceObservation, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	grant := g.owner.GetGrant()
	if g.closing != nil || g.runContext != nil && g.runContext.Err() != nil ||
		grant.GetNamespaceId() != request.NamespaceId ||
		grant.GetEpoch() != request.Owner.Epoch || grant.GetStorageHostId() != request.Owner.StorageHostId ||
		grant.GetProcessBootId() != request.Owner.ProcessBootId {
		return nil, serviceerror.NewUnavailable("Watch runtime does not hold the requested owner")
	}
	if g.view == nil || g.view.generation != request.Generation || g.nodeGeneration != request.Generation ||
		g.nodeObservedAt.IsZero() || now.Sub(g.nodeObservedAt) > 5*time.Second ||
		g.problems["observation"] != "" || g.problems["cleanup"] != "" {
		return nil, serviceerror.NewUnavailable("Watch runtime has no fresh observation of the requested registry cut")
	}
	entries := make([]*watchservice.ResourceObservation, 0, len(request.Targets))
	for _, target := range request.Targets {
		entry := &watchservice.ResourceObservation{Target: common.CloneProto(target)}
		record := g.view.current[resourceKey{observationKind(target.Kind), target.Id}]
		expected, err := watchclient.RecordTarget(record)
		if err != nil || !proto.Equal(expected, target) {
			return nil, serviceerror.NewFailedPrecondition("runtime resource does not match the requested registry cut")
		}
		if target.Kind == watchservice.RESOURCE_KIND_WATCH {
			entry.Status = g.watchStatusLocked(record, now)
		} else {
			entry.Status = g.nodeStatusLocked(record, target.Revision)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (g *Graph) retiredNodeStatusLocked(record *registrypb.Record, revision uint64) *contractpb.ResourceStatus {
	if record.Desired == registrypb.DESIRED_STATE_DELETED {
		id := record.GetSource().GetId()
		if id == "" {
			id = record.GetQuery().GetId()
		}
		for _, node := range g.nodes {
			if node.Status == native.NodeRetired && node.Definition.Lifetime.ResourceID == id &&
				node.Definition.Lifetime.Incarnation == record.Incarnation {
				return &contractpb.ResourceStatus{State: contractpb.LifecycleState_LIFECYCLE_STATE_STOPPED,
					ObservedRevision: revision, UpdatedAt: timestamppb.New(g.nodeObservedAt)}
			}
		}
	}
	return nil
}

func (g *Graph) nodeStatusLocked(record *registrypb.Record, revision uint64) *contractpb.ResourceStatus {
	if retired := g.retiredNodeStatusLocked(record, revision); retired != nil {
		return retired
	}
	definition, _, err := desiredNode(g.view, record, g.nodes, g.temporalRecipe())
	if err != nil {
		return &contractpb.ResourceStatus{
			State:   contractpb.LifecycleState_LIFECYCLE_STATE_FAILED,
			Message: "Runtime cannot reconcile this resource configuration", UpdatedAt: timestamppb.New(g.nodeObservedAt),
		}
	}
	if definition.Recipe == nil {
		return nil
	}
	node, ok := matchingNativeNode(definition, g.nodes)
	if !ok {
		return nil
	}
	status := &contractpb.ResourceStatus{UpdatedAt: timestamppb.New(g.nodeObservedAt)}
	switch node.Status {
	case native.NodeDeferred:
		if record.Desired == registrypb.DESIRED_STATE_STOPPED {
			status.State = contractpb.LifecycleState_LIFECYCLE_STATE_STOPPED
		} else {
			status.State = contractpb.LifecycleState_LIFECYCLE_STATE_STARTING
			status.Message = "Runtime dependencies await plugin materialization"
		}
	case native.NodeIntended, native.NodeStarting, native.NodeReconfiguring:
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_STARTING
	case native.NodeRunning:
		if !nativeDependenciesRunning(node.Definition, g.nodes) {
			return nil
		}
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING
	case native.NodeStopped:
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_STOPPED
	case native.NodeStopping:
		if record.Desired != registrypb.DESIRED_STATE_DELETED {
			return nil
		}
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_DELETING
	case native.NodeFailed:
		status.State, status.Message = contractpb.LifecycleState_LIFECYCLE_STATE_FAILED, "Runtime reconciliation failed; inspect server logs"
	case native.NodeRecoveryRequired:
		status.State, status.Message = contractpb.LifecycleState_LIFECYCLE_STATE_RECOVERY_REQUIRED, "Runtime continuity requires explicit recovery"
	default:
		return nil
	}
	if node.HistoryBlocked {
		status.State, status.Message = contractpb.LifecycleState_LIFECYCLE_STATE_RECOVERY_REQUIRED, "History continuity requires a new source incarnation"
	}
	if status.State == contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING && record.Desired == registrypb.DESIRED_STATE_RUNNING ||
		status.State == contractpb.LifecycleState_LIFECYCLE_STATE_STOPPED && record.Desired == registrypb.DESIRED_STATE_STOPPED {
		status.ObservedRevision = revision
	}
	return status
}

func (g *Graph) watchStatusLocked(record *registrypb.Record, now time.Time) *contractpb.ResourceStatus {
	resource := record.GetWatch()
	watch := g.watches[native.ResourceLifetime{
		NamespaceID: g.options.NamespaceID, ResourceID: resource.Id, Incarnation: record.Incarnation,
	}]
	if watch == nil || watch.generation != g.view.generation || watch.observedAt.IsZero() ||
		now.Sub(watch.observedAt) > 5*time.Second {
		return nil
	}
	observedAt := watch.observedAt
	if g.nodeObservedAt.Before(observedAt) {
		observedAt = g.nodeObservedAt
	}
	status := &contractpb.ResourceStatus{UpdatedAt: timestamppb.New(observedAt), PendingDeliveries: watch.pending}
	switch {
	case watch.problem != "" || watch.intakeErr != "":
		status.State, status.Message = contractpb.LifecycleState_LIFECYCLE_STATE_FAILED, "Watch reconciliation failed; inspect server logs"
	case watch.status == controlpb.STATUS_SATISFIED:
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_COMPLETED
	case watch.status == controlpb.STATUS_CANCELLED:
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_CANCELED
	case watch.status == controlpb.STATUS_TIMED_OUT:
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_EXPIRED
	case watch.status == controlpb.STATUS_PAUSED && watch.quiesced:
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_PAUSED
	case watch.status == controlpb.STATUS_RUNNING && watch.accepting && watch.live:
		if !g.watchQueryRunningLocked(watch, resource) {
			return nil
		}
		status.State = contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING
	default:
		return nil
	}
	switch status.State {
	case contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING:
		if record.Desired == registrypb.DESIRED_STATE_RUNNING {
			status.ObservedRevision = resource.Revision
		}
	case contractpb.LifecycleState_LIFECYCLE_STATE_PAUSED:
		if record.Desired == registrypb.DESIRED_STATE_PAUSED {
			status.ObservedRevision = resource.Revision
		}
	case contractpb.LifecycleState_LIFECYCLE_STATE_COMPLETED, contractpb.LifecycleState_LIFECYCLE_STATE_CANCELED,
		contractpb.LifecycleState_LIFECYCLE_STATE_EXPIRED:
		status.ObservedRevision = resource.Revision
	default:
		// Failed reconciliation does not certify the current desired goal.
	}
	return status
}

func (g *Graph) watchQueryRunningLocked(watch *graphWatch, resource *contractpb.Watch) bool {
	if watch.binding == nil || watch.binding.Binding == nil || watch.binding.Deleted ||
		watch.binding.Binding.Watch != watch.lifetime || watch.binding.Binding.WatchRevision == 0 ||
		watch.binding.Binding.WatchRevision > resource.Revision || !watchDependenciesPermitted(g.view, watch.lifetime) {
		return false
	}
	query, err := g.view.Query(resource.Spec.Query)
	if err != nil || watch.binding.Binding.Query != (native.ResourceLifetime{
		NamespaceID: g.options.NamespaceID, ResourceID: query.GetQuery().Id, Incarnation: query.Incarnation,
	}) || watch.binding.Binding.QueryRevision != resource.Spec.Query.Revision {
		return false
	}
	definition, ready, err := desiredNode(g.view, query, g.nodes, g.temporalRecipe())
	if err != nil || !ready {
		return false
	}
	actual, ok := matchingNativeNode(definition, g.nodes)
	return ok && actual.NativeID == watch.binding.Definition.QueryID &&
		actual.Status == native.NodeRunning && nativeDependenciesRunning(actual.Definition, g.nodes)
}
