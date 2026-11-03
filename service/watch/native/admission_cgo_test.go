//go:build temporal_with_drasi && cgo

package native

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativePluginPreflightRejectsBeforeCredentialReadsOrReservations(t *testing.T) {
	t.Parallel()
	for _, reviewed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unreviewed pair", true: "unloaded descriptors"}[reviewed], func(t *testing.T) {
			t.Parallel()
			pair := ManagedPluginPair{
				Source:    PluginComponentRef{Digest: "sha256:" + strings.Repeat("a", 64), Component: "source"},
				Bootstrap: PluginComponentRef{Digest: "sha256:" + strings.Repeat("b", 64), Component: "bootstrap"},
			}
			config := Config{StateDir: filepath.Join(workspace(t), "owner"), OwnerEpoch: 1, InstanceID: "preflight", StartMode: StartModeDormant}
			if reviewed {
				config.ManagedPlugins = []ManagedPluginPair{pair}
			}
			runtime := openRuntime(t, config)
			definition := NodeDefinition{
				Lifetime: ResourceLifetime{
					NamespaceID: "00000000-0000-4000-8000-000000000001",
					ResourceID:  "external", Incarnation: "1",
				}, Activation: 1, Recipe: PluginSourceRecipe{
					Source: ComponentRecipe{Plugin: pair.Source, Configuration: json.RawMessage(`{}`),
						Secrets: map[string]SecretReference{"connection": {Provider: "p", Key: "k", Version: "v"}}},
					Bootstrap: ComponentRecipe{Plugin: pair.Bootstrap, Configuration: json.RawMessage(`{}`)},
				},
			}
			called := false
			_, err := runtime.AdmitPluginSource(t.Context(), definition, func(context.Context, SecretReference) ([]byte, error) {
				called = true
				return []byte("opaque-test-value"), nil
			})
			require.Error(t, err)
			require.False(t, called)
			require.NotContains(t, err.Error(), "opaque-test-value")
			result, err := runtime.Apply(t.Context(), ObserveNodesCommand{})
			require.NoError(t, err)
			require.Empty(t, result.Nodes)
			_, err = runtime.Apply(t.Context(), AdmitNodeCommand{Definition: definition})
			require.ErrorContains(t, err, "host-authorized credential inputs")
		})
	}
}

func TestNativeCredentialWipeClearsOwnedAllocation(t *testing.T) {
	t.Parallel()
	value := []byte("opaque-test-value")
	wipeCredential(value)
	require.Equal(t, make([]byte, len(value)), value)
	wipeCredential(nil)
}
