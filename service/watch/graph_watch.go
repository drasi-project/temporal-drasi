package watch

import (
	"context"
	"errors"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (g *Graph) watchLocked(lifetime native.ResourceLifetime) *graphWatch {
	watch, ok := g.watches[lifetime]
	if ok {
		return watch
	}
	watch = &graphWatch{lifetime: lifetime}
	g.watches[lifetime] = watch
	g.workers.Add(2)
	go g.controlWatch(watch)
	go g.consumeWatch(watch)
	return watch
}

func (g *Graph) controlWatch(watch *graphWatch) {
	defer g.workers.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(g.runContext, 30*time.Second)
		err := g.reconcileWatch(ctx, watch)
		cancel()
		if err != nil {
			g.mu.Lock()
			watch.problem = err.Error()
			watch.accepting, watch.quiesced = false, false
			if watch.cancel != nil {
				watch.cancel()
			}
			g.mu.Unlock()
		}
		select {
		case <-g.runContext.Done():
			return
		case <-ticker.C:
		}
	}
}

func watchIntent(view *DesiredView, lifetime native.ResourceLifetime) (retired, running bool) {
	record, ok := view.current[resourceKey{registrypb.RESOURCE_KIND_WATCH, lifetime.ResourceID}]
	if !ok || record.Incarnation != lifetime.Incarnation ||
		record.Desired == registrypb.DESIRED_STATE_DELETED ||
		record.Desired == registrypb.DESIRED_STATE_CANCELED {
		return true, false
	}
	return false, record.Desired == registrypb.DESIRED_STATE_RUNNING
}

func watchDependenciesPermitted(view *DesiredView, lifetime native.ResourceLifetime) bool {
	record, ok := view.current[resourceKey{registrypb.RESOURCE_KIND_WATCH, lifetime.ResourceID}]
	if !ok || record.Incarnation != lifetime.Incarnation {
		return false
	}
	query, err := view.Query(record.GetWatch().Spec.Query)
	if err != nil || !resourceRunning(view, registrypb.RESOURCE_KIND_QUERY, query.GetQuery().Id) {
		return false
	}
	for _, ref := range query.GetQuery().Spec.Sources {
		if !resourceRunning(view, registrypb.RESOURCE_KIND_SOURCE, ref.Id) {
			return false
		}
	}
	return true
}

func (g *Graph) reconcileWatch(ctx context.Context, watch *graphWatch) error {
	if ready, err := g.registerWatch(ctx, watch); err != nil || !ready {
		return err
	}
	g.mu.Lock()
	view := g.view
	retired, running := watchIntent(view, watch.lifetime)
	running = running && watchDependenciesPermitted(view, watch.lifetime)
	observation := *watch.binding
	target := watch.target
	nodes := g.nodes
	g.mu.Unlock()
	if target == nil {
		restored, err := view.retainedNativeBinding(ctx, g.options.Registry, observation, g.owner.Grant.Epoch)
		if err != nil {
			return err
		}
		node, ok := nodes[observation.Definition.QueryID]
		if !ok || node.Definition.Lifetime != observation.Binding.Query {
			return serviceerror.NewDataLoss("Watch binding does not match its retained native query lifetime")
		}
		target = &restored
		g.mu.Lock()
		watch.target = target
		g.mu.Unlock()
	}
	state, err := g.claimWatchControl(ctx, target)
	observedAt := time.Now()
	if err != nil {
		return err
	}
	if !retired && !terminal(state.Status) {
		retired, err = g.ownerClosed(ctx, target.Target.Watch)
		if err != nil {
			return err
		}
	}
	node, exists := nodes[observation.Definition.QueryID]
	ready := exists && node.Status == native.NodeRunning && nativeDependenciesRunning(node.Definition, nodes)
	paused := !running || !ready
	state, err = g.setWatchControl(ctx, target, state, retired, paused)
	if err != nil {
		return err
	}
	if terminal(state.Status) && !observation.Deleted {
		if _, err := g.runtime.Apply(ctx, native.UnsubscribeCommand{SubscriptionID: observation.Definition.ID}); err != nil {
			return err
		}
	}
	if observation.Deleted && !terminal(state.Status) {
		return serviceerror.NewFailedPrecondition("native Watch binding is terminal; an explicit reset is required")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.publishWatchControlLocked(watch, view, state, observedAt, !retired && !paused && !observation.Deleted)
	return nil
}

func (g *Graph) ownerClosed(ctx context.Context, watch *contractpb.Watch) (bool, error) {
	once := watch.Spec.GetOnce()
	if once == nil {
		return false, nil
	}
	response, err := g.options.Workflows.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: watch.Namespace, Execution: common.CloneProto(once.Owner),
	})
	if err != nil {
		if serviceerror.ToStatus(err).Code() == codes.NotFound {
			return true, nil
		}
		return false, err
	}
	info := response.GetWorkflowExecutionInfo()
	if info == nil || !proto.Equal(info.Execution, once.Owner) {
		return false, serviceerror.NewDataLoss("condition owner description does not match its exact workflow run")
	}
	switch info.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return false, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return true, nil
	default:
		return false, serviceerror.NewDataLoss("condition owner description has an unsupported workflow status")
	}
}

func (g *Graph) publishWatchControlLocked(watch *graphWatch, view *DesiredView, state *controlpb.State, observedAt time.Time, allowed bool) {
	if g.view != view || g.closing != nil {
		return
	}
	watch.status, watch.generation, watch.problem = state.Status, view.generation, ""
	watch.observedAt, watch.pending = observedAt, uint64(len(state.PendingIds))
	watch.live = state.ProgressPhase == controlpb.PROGRESS_PHASE_LIVE
	watch.quiesced = terminal(state.Status) || state.Status == controlpb.STATUS_PAUSED
	if watch.quiesced {
		watch.intakeErr = ""
	}
	watch.accepting = allowed && state.Status == controlpb.STATUS_RUNNING && state.OwnerEpoch == g.owner.Grant.Epoch
	if !watch.accepting && watch.cancel != nil {
		watch.cancel()
	}
}

func (g *Graph) registerWatch(ctx context.Context, watch *graphWatch) (bool, error) {
	g.mu.Lock()
	view := g.view
	_, running := watchIntent(view, watch.lifetime)
	running = running && watchDependenciesPermitted(view, watch.lifetime)
	if !running {
		watch.accepting = false
		if watch.cancel != nil {
			watch.cancel()
		}
	}
	if watch.binding != nil {
		g.mu.Unlock()
		return true, nil
	}
	if watch.registration == nil {
		if !running {
			watch.quiesced, watch.generation, watch.problem = true, view.generation, ""
			g.mu.Unlock()
			return false, nil
		}
		command, err := g.newWatchRegistrationLocked(watch)
		if err != nil || command == nil {
			g.mu.Unlock()
			return false, err
		}
		watch.registeringQuery, watch.registration = command.QueryID, &command.Binding
	}
	command := native.BindWatchCommand{Binding: *watch.registration, QueryID: watch.registeringQuery}
	g.mu.Unlock()
	bound, err := g.runtime.Apply(ctx, command)
	if err != nil {
		return false, err
	}
	if bound.Subscription == nil || bound.Subscription.Binding == nil ||
		*bound.Subscription.Binding != command.Binding || bound.Subscription.Definition.QueryID != command.QueryID {
		return false, serviceerror.NewDataLoss("native host did not confirm the requested Watch binding")
	}
	g.mu.Lock()
	watch.binding = bound.Subscription
	g.mu.Unlock()
	return false, nil
}

func (g *Graph) newWatchRegistrationLocked(watch *graphWatch) (*native.BindWatchCommand, error) {
	record := g.view.current[resourceKey{registrypb.RESOURCE_KIND_WATCH, watch.lifetime.ResourceID}]
	query, err := g.view.Query(record.GetWatch().Spec.Query)
	if err != nil {
		return nil, err
	}
	definition, ready, err := desiredNode(g.view, query, g.nodes, g.temporalRecipe())
	if err != nil {
		return nil, err
	}
	node, exists := matchingNativeNode(definition, g.nodes)
	if !ready || !exists || node.Status != native.NodeRunning {
		return nil, nil
	}
	metadata, err := g.view.NativeWatchBinding(watch.lifetime.ResourceID)
	if err != nil {
		return nil, err
	}
	return &native.BindWatchCommand{Binding: metadata, QueryID: node.NativeID}, nil
}

func (g *Graph) claimWatchControl(ctx context.Context, target *NativeBinding) (*controlpb.State, error) {
	response, err := g.options.Control.DescribeExecution(ctx, &controlpb.DescribeExecutionRequest{
		NamespaceId: target.Target.NamespaceID, ExecutionId: target.Target.ExecutionID,
	})
	if _, missing := errors.AsType[*serviceerror.NotFound](err); missing {
		definition, encodeErr := proto.MarshalOptions{Deterministic: true}.Marshal(target.Target.Watch)
		if encodeErr != nil {
			return nil, encodeErr
		}
		once := target.Target.Watch.Spec.GetOnce()
		response, err = g.options.Control.CreateExecution(ctx, &controlpb.CreateExecutionRequest{
			NamespaceId: target.Target.NamespaceID, ExecutionId: target.Target.ExecutionID,
			WatchId: target.Target.Watch.Id, Revision: target.Target.Watch.Revision,
			Definition: definition, Once: once != nil, Deadline: common.CloneProto(once.GetDeadline()),
			RequestId: target.Target.ExecutionID,
		})
	}
	if err != nil {
		return nil, err
	}
	state := response.GetState()
	if err := validateGraphControl(target.Target.Watch, state); err != nil {
		return nil, err
	}
	if !terminal(state.Status) && state.OwnerEpoch != g.owner.Grant.Epoch {
		if state.OwnerEpoch > g.owner.Grant.Epoch {
			return nil, serviceerror.NewFailedPrecondition("Watch control belongs to a newer graph owner")
		}
		response, err = g.options.Control.MutateExecution(ctx, &controlpb.MutateExecutionRequest{
			NamespaceId: target.Target.NamespaceID, ExecutionId: target.Target.ExecutionID,
			ExpectedRevision: state.Revision, OwnerEpoch: state.OwnerEpoch,
			Mutation: &controlpb.MutateExecutionRequest_Claim{Claim: &controlpb.ClaimMutation{
				PreviousEpoch: state.OwnerEpoch, NewEpoch: g.owner.Grant.Epoch,
			}},
		})
		if err != nil {
			return nil, err
		}
		state = response.GetState()
		if err := validateGraphControl(target.Target.Watch, state); err != nil {
			return nil, err
		}
		if state.OwnerEpoch != g.owner.Grant.Epoch {
			return nil, serviceerror.NewFailedPrecondition("Watch control did not confirm graph ownership")
		}
	}
	return state, nil
}

func (g *Graph) setWatchControl(ctx context.Context, target *NativeBinding, state *controlpb.State, retired, paused bool) (*controlpb.State, error) {
	if !terminal(state.Status) {
		request := &controlpb.MutateExecutionRequest{
			NamespaceId: target.Target.NamespaceID, ExecutionId: target.Target.ExecutionID,
			ExpectedRevision: state.Revision, OwnerEpoch: g.owner.Grant.Epoch,
		}
		if retired {
			request.Mutation = &controlpb.MutateExecutionRequest_Cancel{Cancel: &emptypb.Empty{}}
		} else if (state.Status == controlpb.STATUS_PAUSED) != paused {
			request.Mutation = &controlpb.MutateExecutionRequest_Pause{Pause: &controlpb.PauseMutation{Paused: paused}}
		}
		if request.Mutation != nil {
			response, err := g.options.Control.MutateExecution(ctx, request)
			if err != nil {
				return nil, err
			}
			state = response.GetState()
			if err := validateGraphControl(target.Target.Watch, state); err != nil {
				return nil, err
			}
		}
	}
	return state, nil
}

func validateGraphControl(watch *contractpb.Watch, state *controlpb.State) error {
	if state.GetRevision() == 0 || state.GetCreationRevision() != watch.Revision ||
		state.GetNamespace() != watch.Namespace || state.GetWatchId() != watch.Id {
		return serviceerror.NewDataLoss("Watch control does not match its original public definition")
	}
	var persisted contractpb.Watch
	if err := proto.Unmarshal(state.Definition, &persisted); err != nil {
		return serviceerror.NewDataLoss("Watch control definition is malformed")
	}
	expected := common.CloneProto(watch)
	expected.DesiredState, persisted.DesiredState = 0, 0
	expected.Status, persisted.Status = nil, nil
	if !proto.Equal(expected, &persisted) {
		return serviceerror.NewDataLoss("Watch control definition differs from its retained native binding")
	}
	if state.Status != controlpb.STATUS_RUNNING && state.Status != controlpb.STATUS_PAUSED && !terminal(state.Status) {
		return serviceerror.NewDataLoss("Watch control has an unsupported status")
	}
	return nil
}

func (g *Graph) consumeWatch(watch *graphWatch) {
	defer g.workers.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	intake := Intake{Control: g.options.Control, Workflows: g.options.Workflows}
	for {
		g.mu.Lock()
		enabled := watch.accepting && g.closing == nil
		target := watch.target
		ctx, cancel := context.WithCancel(g.runContext)
		if enabled {
			watch.cancel = cancel
		}
		g.mu.Unlock()
		var err error
		if enabled {
			var result native.Result
			result, err = g.runtime.Apply(ctx, native.PollWatchCommand{SubscriptionID: target.Subscription.ID})
			if err == nil && result.Event != nil {
				_, err = intake.ConsumeNative(ctx, g.runtime, *target, *result.Event)
			}
		}
		cancel()
		g.mu.Lock()
		if enabled && !errors.Is(err, context.Canceled) {
			watch.intakeErr = ""
			if err != nil {
				watch.intakeErr = err.Error()
			}
		}
		g.mu.Unlock()
		select {
		case <-g.runContext.Done():
			return
		case <-ticker.C:
		}
	}
}
