//go:build temporal_with_drasi && cgo

package native

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestNativeRetirementPersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	config := Config{
		StateDir: filepath.Join(workspace(t), "owner"), OwnerEpoch: 1,
		InstanceID: "retirement", StartMode: StartModeDormant,
	}
	runtime := openRuntime(t, config)
	definition := NodeDefinition{
		Lifetime: ResourceLifetime{
			NamespaceID: "00000000-0000-4000-8000-000000000001", ResourceID: "source", Incarnation: "1",
		},
		Activation: 1, Recipe: TemporalSourceRecipe{},
	}
	admitted, err := runtime.Apply(t.Context(), AdmitNodeCommand{Definition: definition})
	require.NoError(t, err)
	require.NotNil(t, admitted.Node)
	id := admitted.Node.NativeID
	_, err = runtime.Apply(t.Context(), RetireNodeCommand{NativeID: id})
	require.Error(t, err)
	_, err = runtime.Apply(t.Context(), StopNodeCommand{NativeID: id})
	require.NoError(t, err)
	await.Require(t.Context(), t, func(t *await.T) {
		observed, err := runtime.Apply(t.Context(), ObserveNodesCommand{})
		require.NoError(t, err)
		require.Len(t, observed.Nodes, 1)
		require.Equal(t, NodeStopped, observed.Nodes[0].Status)
	}, 5*time.Second, 10*time.Millisecond)
	retired, err := runtime.Apply(t.Context(), RetireNodeCommand{NativeID: id})
	require.NoError(t, err)
	expected := &NodeObservation{NativeID: id, Definition: definition, Status: NodeRetired}
	require.Equal(t, expected, retired.Node)
	repeated, err := runtime.Apply(t.Context(), RetireNodeCommand{NativeID: id})
	require.NoError(t, err)
	require.Equal(t, expected, repeated.Node)
	_, err = runtime.Apply(t.Context(), AdmitNodeCommand{Definition: definition})
	require.ErrorContains(t, err, "retired")
	require.NoError(t, runtime.Close(t.Context()))
	reopened := openRuntime(t, config)
	observed, err := reopened.Apply(t.Context(), ObserveNodesCommand{})
	require.NoError(t, err)
	require.Equal(t, []NodeObservation{*expected}, observed.Nodes)
	_, err = reopened.Apply(t.Context(), AdmitNodeCommand{Definition: definition})
	require.ErrorContains(t, err, "retired")
	definition.Lifetime.Incarnation = "2"
	replacement, err := reopened.Apply(t.Context(), AdmitNodeCommand{Definition: definition})
	require.NoError(t, err)
	require.NotNil(t, replacement.Node)
	require.Equal(t, NodeRunning, replacement.Node.Status)
	require.NotEqual(t, id, replacement.Node.NativeID)
}
