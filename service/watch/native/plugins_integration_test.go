//go:build temporal_with_drasi && cgo && integration

package native

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeConfiguredPostgresPlugins(t *testing.T) {
	t.Parallel()
	path := os.Getenv("TEMPORAL_DRASI_PLUGIN_CONFIG")
	if path == "" {
		t.Skip("requires configured PostgreSQL source and bootstrap catalog pins")
	}
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	var config Config
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&config))
	require.Empty(t, config.Plugins, "the fixture must exercise explicit install/load rather than startup loading")
	require.Empty(t, config.Sources, "this fixture tests loading, not external database connectivity")
	require.Empty(t, config.Queries)
	require.NotEmpty(t, config.Catalogs)
	config.Version = ABIVersion
	config.StateDir = filepath.Join(workspace(t), "owner")
	config.InstanceID = "plugin-fixture"
	config.OwnerEpoch = 1
	config.RestoreFrom = ""
	runtime := openRuntime(t, config)
	catalogs, err := runtime.Apply(t.Context(), ListCatalogsCommand{})
	require.NoError(t, err)
	require.Len(t, catalogs.Catalogs, len(config.Catalogs))
	var sourceID, bootstrapID string
	for _, catalog := range config.Catalogs {
		for _, release := range catalog.Plugins {
			selection := PluginSelection{CatalogID: catalog.ID, PluginID: release.Artifact.ID, Version: release.Version}
			discovered, err := runtime.Apply(t.Context(), InspectPluginCommand{Selection: selection})
			require.NoError(t, err)
			require.NotNil(t, discovered.Plugin)
			require.True(t, discovered.Plugin.Compatible, discovered.Plugin.Message)
			digest := discovered.Plugin.Digest
			installed, err := runtime.Apply(t.Context(), InstallPluginCommand{Selection: selection, Digest: digest})
			require.NoError(t, err)
			require.NotNil(t, installed.Installation)
			require.Equal(t, PluginInstalled, installed.Installation.State)
			require.Nil(t, installed.Installation.VerifiedSignerIdentity)
			repeated, err := runtime.Apply(t.Context(), InstallPluginCommand{Selection: selection, Digest: digest})
			require.NoError(t, err)
			require.Equal(t, installed.Installation.Plugin, repeated.Installation.Plugin)
			require.Equal(t, PluginInstalled, repeated.Installation.State)
			loaded, err := runtime.Apply(t.Context(), LoadPluginCommand{Digest: digest})
			require.NoError(t, err)
			require.NotNil(t, loaded.Installation)
			require.Equal(t, PluginLoaded, loaded.Installation.State)
			require.Nil(t, loaded.Installation.VerifiedSignerIdentity)
			for _, descriptor := range loaded.Installation.Plugin.Descriptors {
				require.NotEmpty(t, descriptor.ConfigurationSchema)
				if descriptor.Component == "postgres" && descriptor.Category == PluginSource {
					sourceID = release.Artifact.ID
				}
				if descriptor.Component == "postgres" && descriptor.Category == PluginBootstrap {
					bootstrapID = release.Artifact.ID
				}
			}
			reloaded, err := runtime.Apply(t.Context(), LoadPluginCommand{Digest: digest})
			require.NoError(t, err)
			require.Equal(t, PluginLoaded, reloaded.Installation.State)
			require.Equal(t, loaded.Installation.Plugin, reloaded.Installation.Plugin)
		}
	}
	require.NotEmpty(t, sourceID, "no actual PostgreSQL source descriptor was loaded")
	require.NotEmpty(t, bootstrapID, "no actual PostgreSQL bootstrap descriptor was loaded")
}
