package watch

import (
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/watch/native"
)

func TestGraphTopologyStopsOnlyUnpinnedLifetimes(t *testing.T) {
	t.Parallel()
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "superseded", true: "historical dependency"}[retained], func(t *testing.T) {
			t.Parallel()
			source, current, watch := desiredRecords()
			previous := common.CloneProto(current)
			current.Incarnation, current.GetQuery().Revision = "2", 2
			source.Desired, current.Desired = registrypb.DESIRED_STATE_RUNNING, registrypb.DESIRED_STATE_RUNNING
			watch.GetWatch().Spec.Query.Revision = 2
			view := &DesiredView{
				namespaceID: "00000000-0000-4000-8000-000000000001", namespace: "ns", generation: 7,
				current: map[resourceKey]*registrypb.Record{
					{registrypb.RESOURCE_KIND_SOURCE, "source"}: source,
					{registrypb.RESOURCE_KIND_QUERY, "query"}:   current,
					{registrypb.RESOURCE_KIND_WATCH, "watch"}:   watch,
				},
				revisions: map[definitionPin]*registrypb.Record{
					{resourceKey{registrypb.RESOURCE_KIND_SOURCE, "source"}, 1}: source,
					{resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}, 2}:   current,
				},
			}
			if retained {
				watch.GetWatch().Spec.Query.Revision = 1
				view.revisions[definitionPin{resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}, 1}] = previous
			}
			nodes := map[string]native.NodeObservation{}
			for _, entry := range []struct {
				id          string
				resource    string
				incarnation string
				recipe      native.NodeRecipe
			}{
				{"s-current", "source", "1", native.TemporalSourceRecipe{}},
				{"s-old", "source", "0", native.TemporalSourceRecipe{}},
				{"q-current", "query", "2", native.QueryRecipe{Language: native.Cypher, Text: current.GetQuery().Spec.Text, Sources: []string{"s-current"}}},
				{"q-old", "query", "1", native.QueryRecipe{Language: native.Cypher, Text: previous.GetQuery().Spec.Text, Sources: []string{"s-current"}}},
			} {
				nodes[entry.id] = native.NodeObservation{NativeID: entry.id, Status: native.NodeRunning, Definition: native.NodeDefinition{
					Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: entry.resource, Incarnation: entry.incarnation},
					Activation: 1, Recipe: entry.recipe,
				}}
			}
			expected := []graphOperation{{key: "stop/s-old", command: native.StopNodeCommand{NativeID: "s-old"}}}
			if !retained {
				expected = append([]graphOperation{{key: "stop/q-old", command: native.StopNodeCommand{NativeID: "q-old"}}}, expected...)
			}
			operations, problems := graphTopology(view, nodes, nil, native.TemporalSourceRecipe{})
			require.Empty(t, problems)
			require.Equal(t, expected, operations)
		})
	}
}

func TestGraphTopologyGatesEveryRetainedAliasBehindHistory(t *testing.T) {
	t.Parallel()
	for _, kind := range []registrypb.ResourceKind{
		registrypb.RESOURCE_KIND_SOURCE, registrypb.RESOURCE_KIND_QUERY,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			t.Parallel()
			source, query, watch := desiredRecords()
			source.Desired, query.Desired = registrypb.DESIRED_STATE_RUNNING, registrypb.DESIRED_STATE_RUNNING
			view, err := ReadDesiredView(testcontext.For(t), &desiredTestClient{t: t, steps: desiredSteps(source, query, watch)},
				"00000000-0000-4000-8000-000000000001", "ns")
			require.NoError(t, err)
			view.current[resourceKey{kind, map[registrypb.ResourceKind]string{
				registrypb.RESOURCE_KIND_SOURCE: "source", registrypb.RESOURCE_KIND_QUERY: "query",
			}[kind]}].Desired = registrypb.DESIRED_STATE_STOPPED
			nodes := make(map[string]native.NodeObservation)
			watches := make(map[native.ResourceLifetime]*graphWatch)
			for _, incarnation := range []string{"1", "2"} {
				sourceID, queryID := "s-"+incarnation, "q-"+incarnation
				nodes[sourceID] = native.NodeObservation{NativeID: sourceID, Status: native.NodeRunning,
					Definition: native.NodeDefinition{
						Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "source", Incarnation: incarnation},
						Activation: 1, Recipe: native.TemporalSourceRecipe{},
					}}
				nodes[queryID] = native.NodeObservation{NativeID: queryID, Status: native.NodeRunning,
					Definition: native.NodeDefinition{
						Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "query", Incarnation: incarnation},
						Activation: 1, Recipe: native.QueryRecipe{Language: native.Cypher, Text: query.GetQuery().Spec.Text, Sources: []string{sourceID}},
					}}
				lifetime := native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "watch", Incarnation: incarnation}
				watches[lifetime] = &graphWatch{
					lifetime: lifetime, generation: view.generation,
					binding: &native.SubscriptionObservation{Definition: native.Subscription{QueryID: queryID}},
				}
			}
			operations, problems := graphTopology(view, nodes, watches, native.TemporalSourceRecipe{})
			require.Empty(t, problems)
			require.Empty(t, operations, "a native stop cannot precede both History barriers")
			for _, watch := range watches {
				watch.quiesced = true
			}
			operations, problems = graphTopology(view, nodes, watches, native.TemporalSourceRecipe{})
			require.Empty(t, problems)
			require.Equal(t, []graphOperation{
				{key: "stop/q-1", command: native.StopNodeCommand{NativeID: "q-1"}},
				{key: "stop/q-2", command: native.StopNodeCommand{NativeID: "q-2"}},
			}, operations)
			for _, id := range []string{"q-1", "q-2"} {
				node := nodes[id]
				node.Status = native.NodeStopped
				nodes[id] = node
			}
			operations, problems = graphTopology(view, nodes, watches, native.TemporalSourceRecipe{})
			require.Empty(t, problems)
			if kind == registrypb.RESOURCE_KIND_SOURCE {
				require.Equal(t, []graphOperation{
					{key: "stop/s-1", command: native.StopNodeCommand{NativeID: "s-1"}},
					{key: "stop/s-2", command: native.StopNodeCommand{NativeID: "s-2"}},
				}, operations)
			} else {
				require.Equal(t, []graphOperation{{key: "stop/s-2", command: native.StopNodeCommand{NativeID: "s-2"}}}, operations)
			}
			for _, watch := range watches {
				watch.generation--
			}
			nodes["q-1"] = native.NodeObservation{
				NativeID: "q-1", Status: native.NodeRunning, Definition: nodes["q-1"].Definition,
			}
			operations, _ = graphTopology(view, nodes, watches, native.TemporalSourceRecipe{})
			require.NotContains(t, operations, graphOperation{key: "stop/q-1", command: native.StopNodeCommand{NativeID: "q-1"}})
		})
	}
}

func TestGraphTopologyRetiresOnlyClosedUnpinnedDependencies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		query       native.NodeStatus
		source      native.NodeStatus
		deleted     bool
		stale       bool
		registering bool
		want        []graphOperation
	}{
		{name: "open binding", query: native.NodeStopped, source: native.NodeStopped},
		{name: "registration uncertainty", query: native.NodeStopped, source: native.NodeStopped, registering: true},
		{name: "old closed observation", query: native.NodeStopped, source: native.NodeStopped, deleted: true, stale: true},
		{name: "closed with accepted deliveries", query: native.NodeStopped, source: native.NodeStopped, deleted: true,
			want: []graphOperation{{key: "retire/q-old", command: native.RetireNodeCommand{NativeID: "q-old"}}}},
		{name: "retirement retry", query: native.NodeRetiring, source: native.NodeStopped, deleted: true,
			want: []graphOperation{{key: "retire/q-old", command: native.RetireNodeCommand{NativeID: "q-old"}}}},
		{name: "dependent retired", query: native.NodeRetired, source: native.NodeStopped, deleted: true,
			want: []graphOperation{{key: "retire/s-old", command: native.RetireNodeCommand{NativeID: "s-old"}}}},
		{name: "all retired", query: native.NodeRetired, source: native.NodeRetired, deleted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			view := &DesiredView{namespaceID: "00000000-0000-4000-8000-000000000001", generation: 7}
			nodes := map[string]native.NodeObservation{
				"s-old": {NativeID: "s-old", Status: tc.source, Definition: native.NodeDefinition{
					Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "source", Incarnation: "1"},
					Activation: 1, Recipe: native.TemporalSourceRecipe{},
				}},
				"q-old": {NativeID: "q-old", Status: tc.query, Definition: native.NodeDefinition{
					Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "query", Incarnation: "1"},
					Activation: 1, Recipe: native.QueryRecipe{Language: native.Cypher, Text: "MATCH (n) RETURN n", Sources: []string{"s-old"}},
				}},
			}
			lifetime := native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "watch", Incarnation: "1"}
			watch := &graphWatch{lifetime: lifetime, generation: view.generation, quiesced: true, pending: 33,
				binding: &native.SubscriptionObservation{Definition: native.Subscription{QueryID: "q-old"}, Deleted: tc.deleted},
			}
			if tc.stale {
				watch.generation--
			}
			if tc.registering {
				watch.binding, watch.registeringQuery = nil, "q-old"
			}
			operations, problems := graphTopology(view, nodes, map[native.ResourceLifetime]*graphWatch{lifetime: watch}, native.TemporalSourceRecipe{})
			require.Empty(t, problems)
			require.Equal(t, tc.want, operations)
		})
	}
}

func TestGraphTopologyDoesNotAdmitUnpinnedHistoricalDefinitions(t *testing.T) {
	t.Parallel()
	source, query, watch := desiredRecords()
	previousSource, previousQuery := common.CloneProto(source), common.CloneProto(query)
	source.Incarnation, source.GetSource().Revision = "2", 2
	query.Incarnation, query.GetQuery().Revision = "2", 2
	query.GetQuery().Spec.Sources[0].Revision = 2
	source.Desired, query.Desired = registrypb.DESIRED_STATE_RUNNING, registrypb.DESIRED_STATE_RUNNING
	watch.Desired = registrypb.DESIRED_STATE_DELETED
	view := &DesiredView{
		namespaceID: "00000000-0000-4000-8000-000000000001", namespace: "ns", generation: 7,
		current: map[resourceKey]*registrypb.Record{
			{registrypb.RESOURCE_KIND_SOURCE, "source"}: source,
			{registrypb.RESOURCE_KIND_QUERY, "query"}:   query,
			{registrypb.RESOURCE_KIND_WATCH, "watch"}:   watch,
		},
		revisions: map[definitionPin]*registrypb.Record{
			{resourceKey{registrypb.RESOURCE_KIND_SOURCE, "source"}, 1}: previousSource,
			{resourceKey{registrypb.RESOURCE_KIND_SOURCE, "source"}, 2}: source,
			{resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}, 1}:   previousQuery,
			{resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}, 2}:   query,
		},
	}
	nodes := map[string]native.NodeObservation{
		"source": {NativeID: "source", Status: native.NodeRunning, Definition: native.NodeDefinition{
			Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "source", Incarnation: "2"},
			Activation: 1, Recipe: native.TemporalSourceRecipe{},
		}},
		"query": {NativeID: "query", Status: native.NodeRunning, Definition: native.NodeDefinition{
			Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "query", Incarnation: "2"},
			Activation: 1, Recipe: native.QueryRecipe{Language: native.Cypher, Text: query.GetQuery().Spec.Text, Sources: []string{"source"}},
		}},
	}
	operations, problems := graphTopology(view, nodes, nil, native.TemporalSourceRecipe{})
	require.Empty(t, problems)
	require.Empty(t, operations)
}

func TestGraphDefinitionAndObservationCopiesDetachQueryBindings(t *testing.T) {
	t.Parallel()
	source, query, watch := desiredRecords()
	view, err := ReadDesiredView(testcontext.For(t), &desiredTestClient{t: t, steps: desiredSteps(source, query, watch)},
		"00000000-0000-4000-8000-000000000001", "ns")
	require.NoError(t, err)
	detached := cloneDesiredView(view)
	require.True(t, equalDesiredViews(view, detached))
	detached.current[resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}].GetQuery().Spec.Sources[0].Id = "changed"
	require.False(t, equalDesiredViews(view, detached))
	require.Equal(t, "source", query.GetQuery().Spec.Sources[0].Id)
	node := native.NodeObservation{Definition: native.NodeDefinition{
		Recipe: native.QueryRecipe{Sources: []string{"native-source"}},
	}}
	detachedNode := cloneNativeNode(node)
	detachedNode.Definition.Recipe.(native.QueryRecipe).Sources[0] = "changed"
	require.Equal(t, []string{"native-source"}, node.Definition.Recipe.(native.QueryRecipe).Sources)
}

func TestGraphAdmissionRejectsMissingLifetimesAndUnsupportedSources(t *testing.T) {
	t.Parallel()
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "external source", true: "legacy lifetime"}[missing], func(t *testing.T) {
			t.Parallel()
			source, query, watch := desiredRecords()
			source.Desired = registrypb.DESIRED_STATE_RUNNING
			if missing {
				source.Incarnation = ""
			} else {
				source.GetSource().Spec.Kind = &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{}}
			}
			view, err := ReadDesiredView(testcontext.For(t), &desiredTestClient{t: t, steps: desiredSteps(source, query, watch)},
				"00000000-0000-4000-8000-000000000001", "ns")
			require.NoError(t, err)
			operations, problems := graphTopology(view, nil, nil, native.TemporalSourceRecipe{})
			require.Empty(t, operations)
			require.Len(t, problems, 1)
		})
	}
}

func TestGraphHistoryFailureStopsDependentsAndDoesNotRebuildGappedInput(t *testing.T) {
	t.Parallel()
	source, query, watch := desiredRecords()
	source.Desired, query.Desired = registrypb.DESIRED_STATE_RUNNING, registrypb.DESIRED_STATE_RUNNING
	view, err := ReadDesiredView(testcontext.For(t), &desiredTestClient{t: t, steps: desiredSteps(source, query, watch)},
		"00000000-0000-4000-8000-000000000001", "ns")
	require.NoError(t, err)
	recipe := native.HistorySourceRecipe{Scope: native.HistoryScope{NamespaceID: view.namespaceID, ShardCount: 1}}
	nodes := map[string]native.NodeObservation{
		"source": {NativeID: "source", Status: native.NodeFailed, HistoryBlocked: true, Definition: native.NodeDefinition{
			Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: source.GetSource().Id, Incarnation: source.Incarnation},
			Activation: 1, Recipe: recipe,
		}},
		"query": {NativeID: "query", Status: native.NodeRunning, Definition: native.NodeDefinition{
			Lifetime:   native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: query.GetQuery().Id, Incarnation: query.Incarnation},
			Activation: 1, Recipe: native.QueryRecipe{Language: native.Cypher, Text: query.GetQuery().Spec.Text, Sources: []string{"source"}},
		}},
	}
	lifetime := native.ResourceLifetime{NamespaceID: view.namespaceID, ResourceID: "watch", Incarnation: watch.Incarnation}
	watches := map[native.ResourceLifetime]*graphWatch{lifetime: {
		lifetime: lifetime, generation: view.generation, binding: &native.SubscriptionObservation{
			Definition: native.Subscription{QueryID: "query"},
		},
	}}
	operations, problems := graphTopology(view, nodes, watches, recipe)
	require.Empty(t, operations)
	require.Empty(t, problems)
	watches[lifetime].quiesced = true
	operations, problems = graphTopology(view, nodes, watches, recipe)
	require.Empty(t, problems)
	require.Equal(t, []graphOperation{{key: "stop/query", command: native.StopNodeCommand{NativeID: "query"}}}, operations)
	stopped := nodes["query"]
	stopped.Status = native.NodeStopped
	nodes["query"] = stopped
	operations, problems = graphTopology(view, nodes, watches, recipe)
	require.Empty(t, problems)
	require.Equal(t, []graphOperation{{key: "stop/source", command: native.StopNodeCommand{NativeID: "source"}}}, operations)
	stopped = nodes["source"]
	stopped.Status = native.NodeStopped
	nodes["source"] = stopped
	operations, problems = graphTopology(view, nodes, watches, recipe)
	require.Empty(t, operations)
	require.Equal(t, map[string]string{"source": "History capture requires a new source incarnation"}, problems)
}
