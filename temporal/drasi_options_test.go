package temporal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/watch"
	"go.temporal.io/server/service/watch/native"
)

type drasiCredentialReader struct {
	called bool
}

func (reader *drasiCredentialReader) ReadSecret(context.Context, namespace.ID, native.SecretReference) ([]byte, error) {
	reader.called = true
	return nil, nil
}

func TestDrasiServerOptionsDoNotReadCredentials(t *testing.T) {
	t.Parallel()
	reader := &drasiCredentialReader{}
	policy := watch.PluginPolicy{
		ManagedPlugins: []native.ManagedPluginPair{{
			Source:    native.PluginComponentRef{Component: "source"},
			Bootstrap: native.PluginComponentRef{Component: "bootstrap"},
		}},
	}
	options := newServerOptions([]ServerOption{WithDrasiPluginPolicy(policy), WithDrasiCredentialReader(reader)})
	require.Equal(t, &policy, options.drasiPluginPolicy)
	require.Same(t, reader, options.drasiCredentials)
	require.False(t, reader.called)
	defaults := newServerOptions(nil)
	require.Nil(t, defaults.drasiPluginPolicy)
	require.Nil(t, defaults.drasiCredentials)
}
