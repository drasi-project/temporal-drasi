//go:build temporal_with_drasi && cgo

package watch

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/watch/native"
)

func TestGraphPluginWorkerUsesSensitiveAdmissionAndRefusesUnreviewedCode(t *testing.T) {
	t.Parallel()
	definition := pluginSourceDefinition()
	runtime, err := native.Open(t.Context(), native.Config{
		StateDir: filepath.Join(t.TempDir(), "owner"), OwnerEpoch: 1,
		InstanceID: definition.Lifetime.NamespaceID, StartMode: native.StartModeDormant,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close(context.Background())) })
	read := false
	graph := &Graph{
		runtime: runtime, runContext: t.Context(), problems: make(map[string]string),
		options: GraphOptions{
			NamespaceID: definition.Lifetime.NamespaceID,
			Credentials: graphCredentialReader(func(context.Context, namespace.ID, native.SecretReference) ([]byte, error) {
				read = true
				return nil, nil
			}),
		},
	}
	job := &graphOperation{key: nativeDefinitionKey(definition), command: native.AdmitNodeCommand{Definition: definition}}
	graph.workers.Add(1)
	graph.applyTopology(job)
	require.False(t, read)
	require.Contains(t, graph.problems[job.key], "administrator-reviewed lifecycle contract")
	require.NotContains(t, graph.problems[job.key], "host-authorized credential inputs")
	result, err := runtime.Apply(t.Context(), native.ObserveNodesCommand{})
	require.NoError(t, err)
	require.Empty(t, result.Nodes)
}
