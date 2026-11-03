package watch

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/types/known/structpb"
)

type graphCredentialReader func(context.Context, namespace.ID, native.SecretReference) ([]byte, error)

func (read graphCredentialReader) ReadSecret(ctx context.Context, id namespace.ID, reference native.SecretReference) ([]byte, error) {
	return read(ctx, id, reference)
}

func pluginSourceDefinition() native.NodeDefinition {
	return native.NodeDefinition{
		Lifetime: native.ResourceLifetime{
			NamespaceID: "00000000-0000-4000-8000-000000000001", ResourceID: "source", Incarnation: "1",
		}, Activation: 1, Recipe: native.PluginSourceRecipe{
			Source: native.ComponentRecipe{
				Plugin:        native.PluginComponentRef{Digest: "sha256:" + strings.Repeat("a", 64), Component: "source"},
				Configuration: json.RawMessage(`{"nested":{"b":2,"a":1}}`),
				Secrets:       map[string]native.SecretReference{"connection": {Provider: "provider", Key: "key", Version: "generation-1"}},
			},
			Bootstrap: native.ComponentRecipe{
				Plugin:        native.PluginComponentRef{Digest: "sha256:" + strings.Repeat("b", 64), Component: "bootstrap"},
				Configuration: json.RawMessage(`{}`),
				Secrets:       map[string]native.SecretReference{"connection": {Provider: "provider", Key: "key", Version: "generation-2"}},
			},
		},
	}
}

func TestGraphPluginTranslationPreservesImmutableReferencesAndPins(t *testing.T) {
	t.Parallel()
	source, query, watch := desiredRecords()
	source.Desired, query.Desired = registrypb.DESIRED_STATE_RUNNING, registrypb.DESIRED_STATE_RUNNING
	expected := pluginSourceDefinition()
	recipe := expected.Recipe.(native.PluginSourceRecipe)
	external := &contractpb.ComponentConfiguration{
		Plugin: &contractpb.PluginComponentRef{Digest: recipe.Source.Plugin.Digest, Component: recipe.Source.Plugin.Component},
		Configuration: &structpb.Struct{Fields: map[string]*structpb.Value{
			"nested": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
				"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2),
			}}),
		}},
		Secrets: map[string]*contractpb.SecretReference{"connection": {Provider: "provider", Key: "key", Version: "generation-1"}},
	}
	source.GetSource().Spec = &contractpb.SourceSpec{
		Kind: &contractpb.SourceSpec_External{External: external},
		Bootstrap: &contractpb.ComponentConfiguration{
			Plugin:  &contractpb.PluginComponentRef{Digest: recipe.Bootstrap.Plugin.Digest, Component: recipe.Bootstrap.Plugin.Component},
			Secrets: map[string]*contractpb.SecretReference{"connection": {Provider: "provider", Key: "key", Version: "generation-2"}},
		},
	}
	view, err := ReadDesiredView(t.Context(), &desiredTestClient{t: t, steps: desiredSteps(source, query, watch)}, expected.Lifetime.NamespaceID, "ns")
	require.NoError(t, err)
	definition, ready, err := desiredNode(view, source, nil, nil)
	require.NoError(t, err)
	require.True(t, ready)
	node := native.NodeObservation{NativeID: "native-source", Status: native.NodeRunning, Definition: expected}
	nodes := map[string]native.NodeObservation{node.NativeID: node}
	actual, found := matchingNativeNode(definition, nodes)
	require.True(t, found)
	require.Equal(t, node, actual)
	dependent, ready, err := desiredNode(view, query, nodes, nil)
	require.NoError(t, err)
	require.True(t, ready)
	require.Equal(t, native.QueryRecipe{
		Language: native.Cypher, Text: query.GetQuery().Spec.Text, Sources: []string{node.NativeID},
	}, dependent.Recipe)
	external.Secrets["connection"].Version = "generation-3"
	require.Equal(t, "generation-1", definition.Recipe.(native.PluginSourceRecipe).Source.Secrets["connection"].Version)
	changed := cloneNativeNode(node)
	changedRecipe := changed.Definition.Recipe.(native.PluginSourceRecipe)
	changedRecipe.Source.Secrets["connection"] = native.SecretReference{Provider: "provider", Key: "key", Version: "generation-3"}
	changed.Definition.Recipe = changedRecipe
	_, found = matchingNativeNode(definition, map[string]native.NodeObservation{changed.NativeID: changed})
	require.False(t, found)
}

func TestGraphPluginLifecycleIncludesDeferredAndExplicitCleanup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		status      native.NodeStatus
		running     bool
		wantAdmit   bool
		wantStop    bool
		wantProblem bool
	}{
		{"deferred running", native.NodeDeferred, true, true, false, false},
		{"deferred stopped", native.NodeDeferred, false, false, false, false},
		{"warm stopped", native.NodeStopped, true, true, false, false},
		{"recovery must be explicit", native.NodeRecoveryRequired, true, false, false, true},
		{"explicit stop requests native cleanup", native.NodeRecoveryRequired, false, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source, _, _ := desiredRecords()
			if tc.running {
				source.Desired = registrypb.DESIRED_STATE_RUNNING
			}
			node := native.NodeObservation{NativeID: "source", Status: tc.status, Definition: pluginSourceDefinition()}
			view := &DesiredView{namespaceID: node.Definition.Lifetime.NamespaceID,
				current: map[resourceKey]*registrypb.Record{{registrypb.RESOURCE_KIND_SOURCE, "source"}: source}}
			dependent := native.NodeObservation{NativeID: "query", Status: native.NodeDeferred, Definition: native.NodeDefinition{
				Recipe: native.QueryRecipe{Sources: []string{node.NativeID}},
			}}
			operation, problem := retainedNodeOperation(view, node, map[string]native.NodeObservation{
				node.NativeID: node, dependent.NativeID: dependent,
			}, nil)
			if tc.wantProblem {
				require.Equal(t, "native node requires explicit recovery", problem)
			} else {
				require.Empty(t, problem)
			}
			if tc.wantAdmit {
				require.Equal(t, &graphOperation{key: nativeDefinitionKey(node.Definition),
					command: native.AdmitNodeCommand{Definition: node.Definition}}, operation)
			} else if tc.wantStop {
				require.Equal(t, &graphOperation{key: "stop/source", command: native.StopNodeCommand{NativeID: "source"}}, operation)
			} else {
				require.Nil(t, operation)
			}
		})
	}
}

func TestGraphPluginObservationsAndPoliciesAreDetached(t *testing.T) {
	t.Parallel()
	node := native.NodeObservation{Definition: pluginSourceDefinition()}
	detached := cloneNativeNode(node)
	for _, component := range []native.ComponentRecipe{
		detached.Definition.Recipe.(native.PluginSourceRecipe).Source,
		detached.Definition.Recipe.(native.PluginSourceRecipe).Bootstrap,
	} {
		component.Configuration[0] = '['
		component.Secrets["connection"] = native.SecretReference{Version: "changed"}
	}
	require.Equal(t, pluginSourceDefinition(), node.Definition)
	policy := PluginPolicy{
		Preload:        []native.Plugin{{ID: "source"}},
		Catalogs:       []native.CatalogConfig{{ID: "catalog", Plugins: []native.CatalogRelease{{Version: "1"}}}},
		Trust:          native.Trust{RegistryPrefixes: []string{"registry"}, Identities: []native.Identity{{Issuer: "issuer", Subject: "subject"}}},
		ManagedPlugins: []native.ManagedPluginPair{{Source: node.Definition.Recipe.(native.PluginSourceRecipe).Source.Plugin}},
	}
	original := clonePluginPolicy(policy)
	clone := clonePluginPolicy(policy)
	clone.Preload[0].ID = "changed"
	clone.Catalogs[0].Plugins[0].Version = "changed"
	clone.Trust.RegistryPrefixes[0] = "changed"
	clone.Trust.Identities[0].Subject = "changed"
	clone.ManagedPlugins[0].Source.Component = "changed"
	require.Equal(t, original, policy)
}

func TestGraphPluginResolverUsesAuthoritativeNamespaceAndExactReference(t *testing.T) {
	t.Parallel()
	definition := pluginSourceDefinition()
	expected := native.SecretReference{Provider: "provider", Key: "key", Version: "generation-2"}
	calls := 0
	graph := &Graph{options: GraphOptions{
		NamespaceID: definition.Lifetime.NamespaceID,
		Credentials: graphCredentialReader(func(ctx context.Context, id namespace.ID, actual native.SecretReference) ([]byte, error) {
			require.NoError(t, ctx.Err())
			require.Equal(t, namespace.ID(definition.Lifetime.NamespaceID), id)
			require.Equal(t, expected, actual)
			calls++
			return nil, nil
		}),
	}}
	resolve, err := graph.pluginResolver(definition)
	require.NoError(t, err)
	_, err = resolve(t.Context(), expected)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	foreign := definition
	foreign.Lifetime.NamespaceID = "00000000-0000-4000-8000-000000000002"
	resolve, err = graph.pluginResolver(foreign)
	require.ErrorContains(t, err, "owned graph namespace")
	require.Nil(t, resolve)
	require.Equal(t, 1, calls)
}

func TestGraphPluginConfigurationComparisonPreservesLargeNumbers(t *testing.T) {
	t.Parallel()
	first := native.ComponentRecipe{Configuration: json.RawMessage(`{"n":9007199254740993,"object":{"b":2,"a":1}}`)}
	second := native.ComponentRecipe{Configuration: json.RawMessage(`{"object":{"a":1,"b":2},"n":9007199254740993}`)}
	require.True(t, samePluginComponent(first, second))
	second.Configuration = json.RawMessage(`{"n":9007199254740992,"object":{"a":1,"b":2}}`)
	require.False(t, samePluginComponent(first, second))
}
