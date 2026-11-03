package native

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeAdmissionCommandEncoding(t *testing.T) {
	t.Parallel()
	lifetime := ResourceLifetime{
		NamespaceID: "00000000-0000-4000-8000-000000000001",
		ResourceID:  "resource", Incarnation: "00000000-0000-4000-8000-000000000002",
	}
	for _, tc := range []struct {
		name     string
		command  Command
		expected string
	}{
		{
			"source",
			AdmitNodeCommand{Definition: NodeDefinition{Lifetime: lifetime, Activation: 1, Recipe: TemporalSourceRecipe{}}},
			`{"op":"admit_node","definition":{"lifetime":{"namespace_id":"00000000-0000-4000-8000-000000000001","resource_id":"resource","incarnation":"00000000-0000-4000-8000-000000000002"},"activation":1,"recipe":{"kind":"temporal_source"}}}`,
		},
		{
			"query",
			AdmitNodeCommand{Definition: NodeDefinition{Lifetime: lifetime, Activation: 2, Recipe: QueryRecipe{
				Language: Cypher, Text: "MATCH (n) RETURN n", Sources: []string{"source-identity"},
			}}},
			`{"op":"admit_node","definition":{"lifetime":{"namespace_id":"00000000-0000-4000-8000-000000000001","resource_id":"resource","incarnation":"00000000-0000-4000-8000-000000000002"},"activation":2,"recipe":{"kind":"query","language":"cypher","text":"MATCH (n) RETURN n","sources":["source-identity"]}}}`,
		},
		{"observations", ObserveNodesCommand{}, `{"op":"observe_nodes"}`},
		{"stop", StopNodeCommand{NativeID: "node"}, `{"op":"stop_node","native_id":"node"}`},
		{"subscriptions", ListSubscriptionsCommand{After: "cursor", Limit: 64}, `{"op":"list_subscriptions","after":"cursor","limit":64}`},
		{"watch poll", PollWatchCommand{SubscriptionID: "watch"}, `{"op":"poll_watch","subscription_id":"watch"}`},
		{
			"watch binding",
			BindWatchCommand{Binding: WatchBinding{Watch: lifetime, WatchRevision: 2, Query: lifetime, QueryRevision: 3}, QueryID: "native-query"},
			`{"op":"bind_watch","binding":{"watch":{"namespace_id":"00000000-0000-4000-8000-000000000001","resource_id":"resource","incarnation":"00000000-0000-4000-8000-000000000002"},"watch_revision":2,"query":{"namespace_id":"00000000-0000-4000-8000-000000000001","resource_id":"resource","incarnation":"00000000-0000-4000-8000-000000000002"},"query_revision":3},"query_id":"native-query"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload, err := marshalCommand(tc.command)
			require.NoError(t, err)
			require.JSONEq(t, tc.expected, string(payload))
		})
	}
	_, err := marshalCommand(AdmitNodeCommand{Definition: NodeDefinition{Lifetime: lifetime, Activation: 1}})
	require.ErrorContains(t, err, "unknown or nil node recipe")
}

func TestCommandEncodingRejectsNil(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		command Command
	}{
		{"nil interface", nil},
		{"nil command pointer", (*FeedCommand)(nil)},
		{"nil History command pointer", (*AcceptHistoryCommand)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload, err := marshalCommand(tc.command)
			require.ErrorContains(t, err, "unknown or nil command")
			require.Nil(t, payload)
		})
	}
}

func TestNodeDefinitionJSONRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		recipe NodeRecipe
	}{
		{"source", TemporalSourceRecipe{}},
		{"History source", HistorySourceRecipe{Scope: HistoryScope{
			NamespaceID: "00000000-0000-4000-8000-000000000001", ShardCount: 4,
		}}},
		{"query", QueryRecipe{Language: GQL, Text: "MATCH (n) RETURN n", Sources: []string{"source"}}},
		{"plugin source", PluginSourceRecipe{
			Source: ComponentRecipe{
				Plugin:        PluginComponentRef{Digest: "sha256:source", Component: "source"},
				Configuration: json.RawMessage(`{"ordinary":"value"}`),
				Secrets:       map[string]SecretReference{"connection": {Provider: "provider", Key: "key", Version: "generation-1"}},
			},
			Bootstrap: ComponentRecipe{
				Plugin:        PluginComponentRef{Digest: "sha256:bootstrap", Component: "bootstrap"},
				Configuration: json.RawMessage(`{}`),
			},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expected := NodeDefinition{
				Lifetime: ResourceLifetime{
					NamespaceID: "00000000-0000-4000-8000-000000000001",
					ResourceID:  "resource.events", Incarnation: "7",
				},
				Activation: 1, Recipe: tc.recipe,
			}
			data, err := json.Marshal(expected)
			require.NoError(t, err)
			var actual NodeDefinition
			require.NoError(t, json.Unmarshal(data, &actual))
			require.Equal(t, expected, actual)
		})
	}
}

func TestNodeDefinitionJSONRejectsInvalidRecipesWithoutChangingReceiver(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data string
	}{
		{"unknown recipe", `{"recipe":{"kind":"external"}}`},
		{"missing recipe", `{}`},
		{"missing History scope", `{"recipe":{"kind":"history_source"}}`},
		{"mixed History recipe", `{"recipe":{"kind":"history_source","scope":{},"text":"MATCH (n) RETURN n"}}`},
		{"source query fields", `{"recipe":{"kind":"temporal_source","text":"MATCH (n) RETURN n"}}`},
		{"wrong field type", `{"activation":"one","recipe":{"kind":"temporal_source"}}`},
		{"missing bootstrap", `{"recipe":{"kind":"plugin_source","source":{}}}`},
		{"mixed plugin recipe", `{"recipe":{"kind":"plugin_source","source":{},"bootstrap":{},"text":"MATCH (n) RETURN n"}}`},
		{"plugin query fields", `{"recipe":{"kind":"query","source":{},"bootstrap":{}}}`},
		{"malformed", `{"recipe":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expected := NodeDefinition{
				Lifetime:   ResourceLifetime{ResourceID: "unchanged"},
				Activation: 9, Recipe: TemporalSourceRecipe{},
			}
			actual := expected
			require.Error(t, json.Unmarshal([]byte(tc.data), &actual))
			require.Equal(t, expected, actual)
		})
	}
}
