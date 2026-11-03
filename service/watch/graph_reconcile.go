package watch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	controlpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/proto"
)

type GraphObservation struct {
	Generation uint64
	Converged  bool
	Nodes      []native.NodeObservation
	Watches    []WatchObservation
	Problems   map[string]string
}

type WatchObservation struct {
	Lifetime       native.ResourceLifetime
	SubscriptionID string
	Status         controlpb.Status
	Quiesced       bool
	Accepting      bool
	Problem        string
}

type graphOperation struct {
	key       string
	command   native.Command
	uncertain bool
}

type graphWatch struct {
	lifetime         native.ResourceLifetime
	binding          *native.SubscriptionObservation
	target           *NativeBinding
	generation       uint64
	status           controlpb.Status
	quiesced         bool
	accepting        bool
	problem          string
	intakeErr        string
	cancel           context.CancelFunc
	registeringQuery string
	registration     *native.WatchBinding
	observedAt       time.Time
	pending          uint64
	live             bool
}

// Reconcile installs a complete cut; native and History convergence is asynchronous.
func (g *Graph) Reconcile(ctx context.Context, view *DesiredView) (GraphObservation, error) {
	if err := ctx.Err(); err != nil {
		return GraphObservation{}, err
	}
	if view == nil || view.namespaceID != g.options.NamespaceID || view.namespace != g.options.Namespace {
		return GraphObservation{}, serviceerror.NewInvalidArgument("desired view does not match the owned graph namespace")
	}
	if g.options.Registry == nil || g.options.Control == nil || g.options.Workflows == nil {
		return GraphObservation{}, serviceerror.NewFailedPrecondition("graph reconciliation requires registry, control and workflow clients")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing != nil {
		return GraphObservation{}, serviceerror.NewFailedPrecondition("Watch graph is closing")
	}
	if g.view != nil && (view.generation < g.view.generation ||
		(view.generation == g.view.generation && !equalDesiredViews(view, g.view))) {
		return GraphObservation{}, serviceerror.NewFailedPrecondition("desired view regressed or changed under the same generation")
	}
	if g.view == nil || view.generation != g.view.generation {
		g.view = cloneDesiredView(view)
		g.observed = false
		for _, watch := range g.watches {
			watch.accepting, watch.quiesced = false, false
			if watch.cancel != nil {
				watch.cancel()
			}
		}
	}
	if g.cancel == nil {
		g.runContext, g.cancel = context.WithCancel(context.Background())
		g.nodes = make(map[string]native.NodeObservation)
		g.watches = make(map[native.ResourceLifetime]*graphWatch)
		g.problems = make(map[string]string)
		g.attempts = make(map[string]time.Time)
		g.history = make(map[string]*historyInputWorker)
		g.workers.Add(1)
		go g.run()
	}
	return g.observationLocked(), nil
}

func cloneDesiredView(view *DesiredView) *DesiredView {
	result := &DesiredView{
		namespaceID: view.namespaceID, namespace: view.namespace, generation: view.generation,
		current: make(map[resourceKey]*registrypb.Record), revisions: make(map[definitionPin]*registrypb.Record),
	}
	for key, value := range view.current {
		result.current[key] = common.CloneProto(value)
	}
	for key, value := range view.revisions {
		result.revisions[key] = common.CloneProto(value)
	}
	return result
}

func equalDesiredViews(a, b *DesiredView) bool {
	return maps.EqualFunc(a.current, b.current, func(a, b *registrypb.Record) bool { return proto.Equal(a, b) }) &&
		maps.EqualFunc(a.revisions, b.revisions, func(a, b *registrypb.Record) bool { return proto.Equal(a, b) })
}

func cloneNativeNode(node native.NodeObservation) native.NodeObservation {
	switch recipe := node.Definition.Recipe.(type) {
	case native.QueryRecipe:
		recipe.Sources = slices.Clone(recipe.Sources)
		node.Definition.Recipe = recipe
	case native.PluginSourceRecipe:
		recipe.Source = clonePluginComponent(recipe.Source)
		recipe.Bootstrap = clonePluginComponent(recipe.Bootstrap)
		node.Definition.Recipe = recipe
	default:
		return node
	}
	return node
}

func (g *Graph) observationLocked() GraphObservation {
	result := GraphObservation{Generation: g.view.generation, Problems: maps.Clone(g.problems)}
	result.Converged = g.observed && g.topology == nil && len(g.problems) == 0
	for id, worker := range g.history {
		if worker.problem != "" {
			result.Problems["history/"+id] = worker.problem
			result.Converged = false
		}
	}
	for _, id := range slices.Sorted(maps.Keys(g.nodes)) {
		result.Nodes = append(result.Nodes, cloneNativeNode(g.nodes[id]))
	}
	for _, watch := range g.watches {
		observation := WatchObservation{
			Lifetime: watch.lifetime, Status: watch.status, Quiesced: watch.quiesced,
			Accepting: watch.accepting, Problem: watch.problem,
		}
		if watch.binding != nil {
			observation.SubscriptionID = watch.binding.Definition.ID
		}
		if watch.intakeErr != "" {
			observation.Problem = watch.intakeErr
		}
		result.Watches = append(result.Watches, observation)
		if watch.generation != g.view.generation || watch.problem != "" || watch.intakeErr != "" ||
			(!watch.accepting && !watch.quiesced) {
			result.Converged = false
		}
	}
	slices.SortFunc(result.Watches, func(a, b WatchObservation) int {
		return bytes.Compare([]byte(a.Lifetime.ResourceID+"\x00"+a.Lifetime.Incarnation),
			[]byte(b.Lifetime.ResourceID+"\x00"+b.Lifetime.Incarnation))
	})
	return result
}

func (g *Graph) run() {
	defer g.workers.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := g.observe(); err != nil {
			g.mu.Lock()
			g.problems["observation"] = err.Error()
			g.observed = false
			for _, watch := range g.watches {
				watch.accepting = false
				if watch.cancel != nil {
					watch.cancel()
				}
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

func (g *Graph) observe() error {
	g.mu.Lock()
	if g.closing != nil {
		g.mu.Unlock()
		return g.runContext.Err()
	}
	previous := g.topology
	view := g.view
	uncertain := previous != nil && previous.uncertain
	g.mu.Unlock()
	result, err := g.runtime.Apply(g.runContext, native.ObserveNodesCommand{})
	observedAt := time.Now()
	if err != nil {
		return err
	}
	if result.TopologyBusy == nil {
		return serviceerror.NewFailedPrecondition("native host does not support topology completion observation")
	}
	nodes, err := graphNativeNodes(g.options.NamespaceID, result.Nodes)
	if err != nil {
		return err
	}
	subscriptions, err := g.subscriptions(g.runContext)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing != nil || g.view != view {
		return nil
	}
	g.nodes = nodes
	g.applyHistoryFailuresLocked()
	nodes = g.nodes
	g.startHistoryWorkersLocked()
	delete(g.problems, "observation")
	if uncertain && g.topology == previous && !*result.TopologyBusy {
		g.topology = nil
	}
	if err := g.installSubscriptionsLocked(subscriptions); err != nil {
		return err
	}
	for key, record := range view.current {
		if key.kind == registrypb.RESOURCE_KIND_WATCH && record.Incarnation != "" {
			g.watchLocked(native.ResourceLifetime{
				NamespaceID: view.namespaceID, ResourceID: key.id, Incarnation: record.Incarnation,
			})
		}
	}
	operations, problems := graphTopology(view, nodes, g.watches, g.temporalRecipe())
	if result.CleanupFailure != nil {
		problems["cleanup"] = *result.CleanupFailure
	}
	g.scheduleTopologyLocked(operations, problems, *result.TopologyBusy)
	g.observed = len(operations) == 0 && len(problems) == 0
	for _, node := range nodes {
		if nativeNodePermitted(view, node, nodes) {
			g.observed = g.observed && node.Status == native.NodeRunning
		} else {
			g.observed = g.observed && (node.Status == native.NodeStopped || node.Status == native.NodeIntended || node.Status == native.NodeDeferred || node.Status == native.NodeRetired)
		}
	}
	g.nodeGeneration, g.nodeObservedAt = view.generation, observedAt
	return nil
}

func graphNativeNodes(namespaceID string, observations []native.NodeObservation) (map[string]native.NodeObservation, error) {
	nodes := make(map[string]native.NodeObservation, len(observations))
	for _, node := range observations {
		if node.NativeID == "" || node.Definition.Lifetime.NamespaceID != namespaceID ||
			node.Definition.Lifetime.Incarnation == "" || node.Definition.Recipe == nil {
			return nil, serviceerror.NewDataLoss("native host returned an incomplete or foreign node")
		}
		if _, duplicate := nodes[node.NativeID]; duplicate {
			return nil, serviceerror.NewDataLoss("native host returned duplicate node identities")
		}
		nodes[node.NativeID] = node
	}
	return nodes, nil
}

func (g *Graph) installSubscriptionsLocked(subscriptions []native.SubscriptionObservation) error {
	for _, subscription := range subscriptions {
		binding := subscription.Binding
		if binding == nil || binding.Watch.NamespaceID != g.options.NamespaceID || binding.Query.NamespaceID != g.options.NamespaceID {
			return serviceerror.NewDataLoss("native host returned an anonymous or foreign Watch binding")
		}
		watch := g.watchLocked(binding.Watch)
		detached := subscription
		metadata := *binding
		detached.Binding = &metadata
		watch.binding = &detached
	}
	return nil
}

func (g *Graph) scheduleTopologyLocked(operations []graphOperation, problems map[string]string, busy bool) {
	for key := range g.problems {
		if key != "observation" && !slices.ContainsFunc(operations, func(operation graphOperation) bool { return operation.key == key }) {
			delete(g.problems, key)
		}
	}
	for key, problem := range problems {
		g.problems[key] = problem
	}
	if g.topology == nil && !busy {
		for _, operation := range operations {
			if time.Since(g.attempts[operation.key]) < time.Second {
				continue
			}
			job := operation
			g.topology = &job
			g.attempts[job.key] = time.Now()
			g.workers.Add(1)
			go g.applyTopology(&job)
			break
		}
	}
}

func (g *Graph) applyTopology(job *graphOperation) {
	defer g.workers.Done()
	var err error
	var stopping string
	switch command := job.command.(type) {
	case native.StopNodeCommand:
		stopping = command.NativeID
	case native.RetireNodeCommand:
		stopping = command.NativeID
	default:
	}
	if stopping != "" {
		err = g.stopHistoryWorker(g.runContext, stopping)
	}
	if err == nil {
		admission, admitting := job.command.(native.AdmitNodeCommand)
		if _, plugin := admission.Definition.Recipe.(native.PluginSourceRecipe); admitting && plugin {
			var resolve native.SecretResolver
			resolve, err = g.pluginResolver(admission.Definition)
			if err == nil {
				_, err = g.runtime.AdmitPluginSource(g.runContext, admission.Definition, resolve)
			}
		} else {
			_, err = g.runtime.Apply(g.runContext, job.command)
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err != nil {
		g.problems[job.key] = err.Error()
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			job.uncertain = true
			return
		}
	} else {
		if stopping != "" {
			delete(g.history, stopping)
		}
		delete(g.problems, job.key)
	}
	g.topology = nil
}

func (g *Graph) subscriptions(ctx context.Context) ([]native.SubscriptionObservation, error) {
	var result []native.SubscriptionObservation
	after := ""
	for {
		page, err := g.runtime.Apply(ctx, native.ListSubscriptionsCommand{After: after, Limit: 64})
		if err != nil {
			return nil, err
		}
		if len(page.Subscriptions) > 64 || len(result)+len(page.Subscriptions) > 10_000 ||
			(len(page.Subscriptions) == 0 && page.NextPageToken != "") {
			return nil, serviceerror.NewDataLoss("native host returned an invalid subscription page")
		}
		for _, subscription := range page.Subscriptions {
			if subscription.Definition.ID <= after {
				return nil, serviceerror.NewDataLoss("native subscription pages are not strictly ordered")
			}
			after = subscription.Definition.ID
			result = append(result, subscription)
		}
		if page.NextPageToken == "" {
			return result, nil
		}
		if page.NextPageToken != after {
			return nil, serviceerror.NewDataLoss("native subscription cursor does not match its page")
		}
	}
}

// FeedTemporal targets one retained public source lifetime, not a native path.
func (g *Graph) FeedTemporal(ctx context.Context, lifetime native.ResourceLifetime, position uint64, change native.Change) error {
	g.mu.Lock()
	if g.closing != nil || g.view == nil || lifetime.NamespaceID != g.options.NamespaceID {
		g.mu.Unlock()
		return serviceerror.NewFailedPrecondition("Watch graph is not accepting Temporal source changes")
	}
	var id string
	for _, node := range g.nodes {
		if _, temporal := node.Definition.Recipe.(native.TemporalSourceRecipe); temporal &&
			node.Definition.Lifetime == lifetime && node.Definition.Activation == 1 &&
			node.Status == native.NodeRunning && nativeNodePermitted(g.view, node, g.nodes) {
			id = node.NativeID
			break
		}
	}
	if id == "" {
		g.mu.Unlock()
		return serviceerror.NewFailedPrecondition("Temporal source lifetime is not running")
	}
	g.workers.Add(1)
	g.mu.Unlock()
	defer g.workers.Done()
	_, err := g.runtime.Apply(ctx, native.FeedCommand{SourceID: id, Position: position, Change: change})
	return err
}

func nativeDefinitionKey(definition native.NodeDefinition) string {
	kind := "source"
	if _, query := definition.Recipe.(native.QueryRecipe); query {
		kind = "query"
	}
	return fmt.Sprintf("admit/%s/%s/%s/%d", kind, definition.Lifetime.ResourceID, definition.Lifetime.Incarnation, definition.Activation)
}
