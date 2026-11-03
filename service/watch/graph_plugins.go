package watch

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"slices"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/encoding/protojson"
)

// PluginPolicy is administrator-owned startup policy, not namespace desired state.
type PluginPolicy struct {
	Preload        []native.Plugin
	Catalogs       []native.CatalogConfig
	Trust          native.Trust
	ManagedPlugins []native.ManagedPluginPair
}

// CredentialReader must authorize the namespace UUID and the complete immutable
// reference, without selecting a newer version. It must honor cancellation and
// return caller-owned bytes, at most 16 KiB, including any partial result on error.
type CredentialReader interface {
	ReadSecret(context.Context, namespace.ID, native.SecretReference) ([]byte, error)
}

func clonePluginPolicy(policy PluginPolicy) PluginPolicy {
	policy.Preload = slices.Clone(policy.Preload)
	policy.Catalogs = slices.Clone(policy.Catalogs)
	for index := range policy.Catalogs {
		policy.Catalogs[index].Plugins = slices.Clone(policy.Catalogs[index].Plugins)
	}
	policy.Trust.RegistryPrefixes = slices.Clone(policy.Trust.RegistryPrefixes)
	policy.Trust.Identities = slices.Clone(policy.Trust.Identities)
	policy.ManagedPlugins = slices.Clone(policy.ManagedPlugins)
	return policy
}

func (g *Graph) updatePlugins(view *pluginIntent, logger log.Logger) error {
	g.mu.Lock()
	if g.closing != nil {
		g.mu.Unlock()
		return nil
	}
	if g.plugins == nil {
		g.plugins = newRuntimePlugins(g.runtime, g.options.NamespaceID, false, logger)
	}
	controller := g.plugins
	g.mu.Unlock()
	return controller.update(view)
}

func desiredSourceRecipe(spec *contractpb.SourceSpec, temporal native.NodeRecipe) (native.NodeRecipe, error) {
	if spec == nil {
		return nil, serviceerror.NewDataLoss("native source definition is missing")
	}
	if spec.GetExternal() != nil {
		return externalSourceRecipe(spec)
	}
	if spec.GetTemporal() == nil || spec.Bootstrap != nil {
		return nil, serviceerror.NewFailedPrecondition("Temporal source requires built-in bootstrap")
	}
	if temporal == nil {
		return nil, serviceerror.NewFailedPrecondition("Temporal source input is not configured")
	}
	return temporal, nil
}

func externalSourceRecipe(spec *contractpb.SourceSpec) (native.PluginSourceRecipe, error) {
	source, err := pluginComponent(spec.GetExternal())
	if err != nil {
		return native.PluginSourceRecipe{}, err
	}
	bootstrap, err := pluginComponent(spec.Bootstrap)
	if err != nil {
		return native.PluginSourceRecipe{}, err
	}
	return native.PluginSourceRecipe{Source: source, Bootstrap: bootstrap}, nil
}

func pluginComponent(configuration *contractpb.ComponentConfiguration) (native.ComponentRecipe, error) {
	if configuration == nil || configuration.Plugin == nil {
		return native.ComponentRecipe{}, serviceerror.NewFailedPrecondition("external sources require pinned source and bootstrap components")
	}
	component := native.ComponentRecipe{
		Plugin:        native.PluginComponentRef{Digest: configuration.Plugin.Digest, Component: configuration.Plugin.Component},
		Configuration: json.RawMessage(`{}`),
	}
	if configuration.Configuration != nil {
		value, err := protojson.Marshal(configuration.Configuration)
		if err != nil {
			return native.ComponentRecipe{}, serviceerror.NewDataLoss("external source contains invalid configuration")
		}
		component.Configuration = value
	}
	if len(configuration.Secrets) != 0 {
		component.Secrets = make(map[string]native.SecretReference, len(configuration.Secrets))
	}
	for field, reference := range configuration.Secrets {
		if reference == nil || reference.Provider == "" || reference.Key == "" || reference.Version == "" {
			return native.ComponentRecipe{}, serviceerror.NewFailedPrecondition("external source credentials require immutable provider, key and version references")
		}
		component.Secrets[field] = native.SecretReference{Provider: reference.Provider, Key: reference.Key, Version: reference.Version}
	}
	return component, nil
}

func clonePluginComponent(component native.ComponentRecipe) native.ComponentRecipe {
	component.Configuration = slices.Clone(component.Configuration)
	component.Secrets = maps.Clone(component.Secrets)
	return component
}

func samePluginComponent(a, b native.ComponentRecipe) bool {
	if a.Plugin != b.Plugin || !maps.Equal(a.Secrets, b.Secrets) {
		return false
	}
	canonical := func(value json.RawMessage) ([]byte, error) {
		var decoded any
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err != nil {
			return nil, err
		}
		return json.Marshal(decoded)
	}
	first, err := canonical(a.Configuration)
	if err != nil {
		return false
	}
	second, err := canonical(b.Configuration)
	return err == nil && bytes.Equal(first, second)
}

func (g *Graph) pluginResolver(definition native.NodeDefinition) (native.SecretResolver, error) {
	if definition.Lifetime.NamespaceID != g.options.NamespaceID {
		return nil, serviceerror.NewDataLoss("plugin admission does not match the owned graph namespace")
	}
	if g.options.Credentials == nil {
		return nil, nil
	}
	id := namespace.ID(g.options.NamespaceID)
	return func(ctx context.Context, reference native.SecretReference) ([]byte, error) {
		return g.options.Credentials.ReadSecret(ctx, id, reference)
	}, nil
}
