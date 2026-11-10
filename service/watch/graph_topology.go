package watch

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/service/watch/native"
)

func resourceRunning(view *DesiredView, kind registrypb.ResourceKind, id string) bool {
	record, ok := view.current[resourceKey{kind, id}]
	return ok && record.Desired == registrypb.DESIRED_STATE_RUNNING
}

func nativeNodePermitted(view *DesiredView, node native.NodeObservation, nodes map[string]native.NodeObservation) bool {
	if node.Status == native.NodeRetired || node.Status == native.NodeRetiring || !nativeNodeRetained(view, node) {
		return false
	}
	switch recipe := node.Definition.Recipe.(type) {
	case native.TemporalSourceRecipe, native.HistorySourceRecipe, native.PluginSourceRecipe:
		return resourceRunning(view, registrypb.RESOURCE_KIND_SOURCE, node.Definition.Lifetime.ResourceID)
	case native.QueryRecipe:
		if !resourceRunning(view, registrypb.RESOURCE_KIND_QUERY, node.Definition.Lifetime.ResourceID) {
			return false
		}
		for _, id := range recipe.Sources {
			source, ok := nodes[id]
			if !ok || source.Status != native.NodeRunning || !resourceRunning(view, registrypb.RESOURCE_KIND_SOURCE, source.Definition.Lifetime.ResourceID) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func nativeNodeRetained(view *DesiredView, node native.NodeObservation) bool {
	_, queryNode := node.Definition.Recipe.(native.QueryRecipe)
	kind := registrypb.RESOURCE_KIND_SOURCE
	if queryNode {
		kind = registrypb.RESOURCE_KIND_QUERY
	}
	return resourceLifetimeRetained(view, kind, node.Definition.Lifetime)
}

func resourceLifetimeRetained(view *DesiredView, kind registrypb.ResourceKind, lifetime native.ResourceLifetime) bool {
	queryNode := kind == registrypb.RESOURCE_KIND_QUERY
	matches := func(record *registrypb.Record) bool {
		return record != nil && lifetime.NamespaceID == view.namespaceID &&
			record.Incarnation == lifetime.Incarnation
	}
	current := view.current[resourceKey{kind, lifetime.ResourceID}]
	if current != nil && current.Desired != registrypb.DESIRED_STATE_DELETED && matches(current) {
		return true
	}
	for _, record := range view.current {
		if record.Desired == registrypb.DESIRED_STATE_DELETED || record.Desired == registrypb.DESIRED_STATE_CANCELED {
			continue
		}
		query := record.GetQuery()
		if watch := record.GetWatch(); watch != nil {
			ref := watch.Spec.Query
			pinned := view.revisions[definitionPin{resourceKey{registrypb.RESOURCE_KIND_QUERY, ref.Id}, ref.Revision}]
			if queryNode && ref.Id == lifetime.ResourceID && matches(pinned) {
				return true
			}
			query = pinned.GetQuery()
		}
		if !queryNode && query != nil {
			for _, ref := range query.Spec.Sources {
				pinned := view.revisions[definitionPin{resourceKey{registrypb.RESOURCE_KIND_SOURCE, ref.Id}, ref.Revision}]
				if ref.Id == lifetime.ResourceID && matches(pinned) {
					return true
				}
			}
		}
	}
	return false
}

func graphTopology(
	view *DesiredView,
	nodes map[string]native.NodeObservation,
	watches map[native.ResourceLifetime]*graphWatch,
	temporal native.NodeRecipe,
) ([]graphOperation, map[string]string) {
	var stops, admissions []graphOperation
	problems := make(map[string]string)
	planned := make(map[string]struct{})
	for _, id := range slices.Sorted(maps.Keys(nodes)) {
		node := nodes[id]
		operation, problem := retainedNodeOperation(view, node, nodes, watches)
		if problem != "" {
			problems[id] = problem
		}
		if operation == nil {
			continue
		}
		if _, stopping := operation.command.(native.StopNodeCommand); stopping {
			stops = append(stops, *operation)
		} else {
			admissions = append(admissions, *operation)
			planned[operation.key] = struct{}{}
		}
	}
	for _, pin := range slices.SortedFunc(maps.Keys(view.revisions), func(a, b definitionPin) int {
		return cmp.Or(cmp.Compare(a.kind, b.kind), cmp.Compare(a.id, b.id), cmp.Compare(a.revision, b.revision))
	}) {
		if pin.kind == registrypb.RESOURCE_KIND_WATCH {
			if view.revisions[pin].Incarnation == "" {
				problems["watch/"+pin.id] = "Watch reconciliation requires a retained definition lifetime"
			}
			continue
		}
		if !resourceRunning(view, pin.kind, pin.id) {
			continue
		}
		record := view.revisions[pin]
		if !resourceLifetimeRetained(view, pin.kind, native.ResourceLifetime{
			NamespaceID: view.namespaceID, ResourceID: pin.id, Incarnation: record.Incarnation,
		}) {
			continue
		}
		definition, ready, err := desiredNode(view, record, nodes, temporal)
		if err != nil {
			problems[fmt.Sprintf("definition/%d/%s/%d", pin.kind, pin.id, pin.revision)] = err.Error()
			continue
		}
		if !ready {
			continue
		}
		key := nativeDefinitionKey(definition)
		if _, ok := planned[key]; ok {
			continue
		}
		if _, ok := matchingNativeNode(definition, nodes); ok {
			continue
		}
		admissions = append(admissions, graphOperation{key: key, command: native.AdmitNodeCommand{Definition: definition}})
		planned[key] = struct{}{}
	}
	return append(stops, admissions...), problems
}

func retainedNodeOperation(view *DesiredView, node native.NodeObservation, nodes map[string]native.NodeObservation,
	watches map[native.ResourceLifetime]*graphWatch,
) (*graphOperation, string) {
	permitted := nativeNodePermitted(view, node, nodes)
	if node.Status == native.NodeRetired {
		if nativeNodeRetained(view, node) {
			return nil, "desired definition references a permanently retired native lifetime"
		}
		return nil, ""
	}
	if node.Status == native.NodeRetiring {
		return &graphOperation{key: "retire/" + node.NativeID, command: native.RetireNodeCommand{NativeID: node.NativeID}}, ""
	}
	if node.Status == native.NodeRecoveryRequired || node.Status == native.NodeReconfiguring {
		if _, plugin := node.Definition.Recipe.(native.PluginSourceRecipe); plugin && !permitted &&
			node.Status == native.NodeRecoveryRequired && nativeStopSafe(view, node, nodes, watches) {
			return &graphOperation{key: "stop/" + node.NativeID, command: native.StopNodeCommand{NativeID: node.NativeID}}, ""
		}
		return nil, "native node requires explicit recovery"
	}
	if permitted && node.HistoryBlocked && node.Status == native.NodeStopped {
		return nil, "History capture requires a new source incarnation"
	}
	if !nativeNodeRetained(view, node) && nativeNodeDormant(node.Status) &&
		nativeRetireSafe(view, node, nodes, watches) {
		return &graphOperation{key: "retire/" + node.NativeID, command: native.RetireNodeCommand{NativeID: node.NativeID}}, ""
	}
	if (!permitted || node.Status == native.NodeFailed) &&
		!nativeNodeDormant(node.Status) {
		if node.Status != native.NodeStopping && nativeStopSafe(view, node, nodes, watches) {
			return &graphOperation{key: "stop/" + node.NativeID, command: native.StopNodeCommand{NativeID: node.NativeID}}, ""
		}
		return nil, ""
	}
	if permitted && nativeNodeDormant(node.Status) &&
		nativeDependenciesRunning(node.Definition, nodes) {
		return &graphOperation{key: nativeDefinitionKey(node.Definition), command: native.AdmitNodeCommand{Definition: node.Definition}}, ""
	}
	return nil, ""
}

func nativeNodeDormant(status native.NodeStatus) bool {
	return status == native.NodeStopped || status == native.NodeIntended || status == native.NodeDeferred
}

func nativeRetireSafe(view *DesiredView, node native.NodeObservation, nodes map[string]native.NodeObservation,
	watches map[native.ResourceLifetime]*graphWatch,
) bool {
	if _, query := node.Definition.Recipe.(native.QueryRecipe); query {
		for _, watch := range watches {
			if watch.registeringQuery == node.NativeID && watch.binding == nil {
				return false
			}
			if watch.binding != nil && watch.binding.Definition.QueryID == node.NativeID &&
				(!watch.binding.Deleted || !watch.quiesced || watch.generation != view.generation) {
				return false
			}
		}
		return true
	}
	for _, dependent := range nodes {
		if recipe, ok := dependent.Definition.Recipe.(native.QueryRecipe); ok &&
			slices.Contains(recipe.Sources, node.NativeID) && dependent.Status != native.NodeRetired {
			return false
		}
	}
	return true
}

func nativeStopSafe(view *DesiredView, node native.NodeObservation, nodes map[string]native.NodeObservation,
	watches map[native.ResourceLifetime]*graphWatch,
) bool {
	if _, query := node.Definition.Recipe.(native.QueryRecipe); query {
		for _, watch := range watches {
			if (watch.binding != nil && watch.binding.Definition.QueryID == node.NativeID &&
				(!watch.quiesced || watch.generation != view.generation)) ||
				(watch.registeringQuery == node.NativeID && watch.binding == nil) {
				return false
			}
		}
		return true
	}
	for _, dependent := range nodes {
		recipe, ok := dependent.Definition.Recipe.(native.QueryRecipe)
		if ok && slices.Contains(recipe.Sources, node.NativeID) &&
			dependent.Status != native.NodeStopped && dependent.Status != native.NodeIntended && dependent.Status != native.NodeDeferred &&
			dependent.Status != native.NodeRetired {
			return false
		}
	}
	return true
}

func desiredNode(view *DesiredView, record *registrypb.Record, nodes map[string]native.NodeObservation, temporal native.NodeRecipe) (native.NodeDefinition, bool, error) {
	if record.Incarnation == "" {
		return native.NodeDefinition{}, false, serviceerror.NewFailedPrecondition("native admission requires a retained definition lifetime")
	}
	definition := native.NodeDefinition{Activation: 1, Lifetime: native.ResourceLifetime{
		NamespaceID: view.namespaceID, Incarnation: record.Incarnation,
	}}
	if source := record.GetSource(); source != nil {
		definition.Lifetime.ResourceID = source.Id
		var err error
		definition.Recipe, err = desiredSourceRecipe(source.Spec, temporal)
		return definition, err == nil, err
	}
	query := record.GetQuery()
	if query == nil || query.Spec == nil {
		return definition, false, serviceerror.NewDataLoss("native query definition is missing")
	}
	definition.Lifetime.ResourceID = query.Id
	if query.Spec.StateStore != "persistent" || (query.Spec.Language != string(native.Cypher) && query.Spec.Language != string(native.GQL)) {
		return definition, false, serviceerror.NewFailedPrecondition("query requires a supported language and persistent state store")
	}
	recipe := native.QueryRecipe{Language: native.Language(query.Spec.Language), Text: query.Spec.Text}
	for _, ref := range query.Spec.Sources {
		if !resourceRunning(view, registrypb.RESOURCE_KIND_SOURCE, ref.Id) {
			return definition, false, nil
		}
		source, err := view.Source(ref)
		if err != nil {
			return definition, false, err
		}
		dependency, _, err := desiredNode(view, source, nodes, temporal)
		if err != nil {
			return definition, false, err
		}
		actual, ok := matchingNativeNode(dependency, nodes)
		if !ok || actual.Status != native.NodeRunning {
			return definition, false, nil
		}
		recipe.Sources = append(recipe.Sources, actual.NativeID)
	}
	definition.Recipe = recipe
	return definition, true, nil
}

func matchingNativeNode(definition native.NodeDefinition, nodes map[string]native.NodeObservation) (native.NodeObservation, bool) {
	for _, node := range nodes {
		if node.Definition.Lifetime != definition.Lifetime || node.Definition.Activation != definition.Activation {
			continue
		}
		switch recipe := definition.Recipe.(type) {
		case native.TemporalSourceRecipe:
			if _, ok := node.Definition.Recipe.(native.TemporalSourceRecipe); ok {
				return node, true
			}
		case native.HistorySourceRecipe:
			if other, ok := node.Definition.Recipe.(native.HistorySourceRecipe); ok && recipe.Scope == other.Scope {
				return node, true
			}
		case native.QueryRecipe:
			other, ok := node.Definition.Recipe.(native.QueryRecipe)
			if ok && recipe.Language == other.Language && recipe.Text == other.Text && slices.Equal(recipe.Sources, other.Sources) {
				return node, true
			}
		case native.PluginSourceRecipe:
			other, ok := node.Definition.Recipe.(native.PluginSourceRecipe)
			if ok && samePluginComponent(recipe.Source, other.Source) && samePluginComponent(recipe.Bootstrap, other.Bootstrap) {
				return node, true
			}
		default:
			return native.NodeObservation{}, false
		}
	}
	return native.NodeObservation{}, false
}

func nativeDependenciesRunning(definition native.NodeDefinition, nodes map[string]native.NodeObservation) bool {
	recipe, query := definition.Recipe.(native.QueryRecipe)
	if !query {
		return true
	}
	for _, id := range recipe.Sources {
		if node, ok := nodes[id]; !ok || node.Status != native.NodeRunning {
			return false
		}
	}
	return true
}
