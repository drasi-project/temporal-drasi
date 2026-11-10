//go:build temporal_with_drasi && cgo && integration

package native

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativePostgresQueryFixture(t *testing.T) {
	t.Parallel()
	container := os.Getenv("TEMPORAL_DRASI_POSTGRES_CONTAINER")
	configPath := os.Getenv("TEMPORAL_DRASI_PLUGIN_CONFIG")
	if container == "" || configPath == "" {
		t.Skip("run native/drasi/verify-postgres.sh for the PostgreSQL fixture")
	}
	bytes, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var config Config
	require.NoError(t, json.Unmarshal(bytes, &config))
	require.Empty(t, config.Plugins)
	require.Empty(t, config.Sources)
	require.Empty(t, config.Queries)
	root := workspace(t)
	config.StateDir = filepath.Join(root, "catalog")
	config.OwnerEpoch = 1
	config.InstanceID = "postgres-catalog-verification"
	catalogRuntime := openRuntime(t, config)
	var source, bootstrap *Plugin
	for _, catalog := range config.Catalogs {
		for _, release := range catalog.Plugins {
			// Each result is needed before constructing the shared source/query generation.
			t.Run(release.Artifact.ID, func(t *testing.T) {
				selection := PluginSelection{CatalogID: catalog.ID, PluginID: release.Artifact.ID, Version: release.Version}
				discovered, err := catalogRuntime.Apply(t.Context(), InspectPluginCommand{Selection: selection})
				require.NoError(t, err)
				require.NotNil(t, discovered.Plugin)
				t.Logf("immutable artifact: %+v", *discovered.Plugin)
				require.True(t, discovered.Plugin.Compatible, discovered.Plugin.Message)
				installed, err := catalogRuntime.Apply(t.Context(), InstallPluginCommand{
					Selection: selection, Digest: discovered.Plugin.Digest,
				})
				require.NoError(t, err)
				require.NotNil(t, installed.Installation)
				require.Equal(t, PluginInstalled, installed.Installation.State)
				require.Nil(t, installed.Installation.VerifiedSignerIdentity)
				loaded, err := catalogRuntime.Apply(t.Context(), LoadPluginCommand{Digest: discovered.Plugin.Digest})
				if err != nil {
					observed, observationErr := catalogRuntime.Apply(t.Context(), GetPluginInstallationCommand{
						Digest: discovered.Plugin.Digest,
					})
					require.NoError(t, observationErr)
					require.NotNil(t, observed.Installation)
					require.NotEqual(t, PluginLoaded, observed.Installation.State)
					t.Logf("rejected load observation: %+v", *observed.Installation)
				}
				require.NoError(t, err)
				require.NotNil(t, loaded.Installation)
				require.Equal(t, PluginLoaded, loaded.Installation.State)
				for _, descriptor := range loaded.Installation.Plugin.Descriptors {
					if descriptor.Component != "postgres" {
						continue
					}
					if descriptor.Category == PluginSource {
						artifact := release.Artifact
						source = &artifact
					}
					if descriptor.Category == PluginBootstrap {
						artifact := release.Artifact
						bootstrap = &artifact
					}
				}
			})
		}
	}
	if t.Failed() {
		return
	}
	require.NotNil(t, source, "a real PostgreSQL source must be loaded")
	require.NotNil(t, bootstrap, "a real PostgreSQL bootstrap must be loaded")
	require.NoError(t, catalogRuntime.Close(t.Context()))
	port, err := strconv.Atoi(os.Getenv("TEMPORAL_DRASI_POSTGRES_PORT"))
	require.NoError(t, err)
	connection, err := json.Marshal(map[string]any{
		"host": "127.0.0.1", "port": port, "database": "watchfixture", "user": "postgres",
		"password": "verification-only-password", "tables": []string{"watch_items"},
		"slotName": "drasi_fixture_slot", "publicationName": "drasi_fixture_pub", "sslMode": "disable",
	})
	require.NoError(t, err)
	config.StateDir = filepath.Join(root, "query")
	config.InstanceID = "postgres-query-verification"
	config.Plugins = []Plugin{*source}
	if source.ID != bootstrap.ID {
		config.Plugins = append(config.Plugins, *bootstrap)
	}
	config.Sources = []Source{{
		ID: "postgres", Kind: SourcePlugin, Plugin: source.ID, PluginKind: "postgres", Config: connection,
		Bootstrap: &Bootstrap{Plugin: bootstrap.ID, Kind: "postgres", Config: connection},
	}}
	config.Queries = []Query{{
		ID: "q", Revision: 1, Language: Cypher, Sources: []string{"postgres"},
		Text: "MATCH (n:watch_items) WHERE n.amount >= 10 RETURN n.name AS name, n.amount * 2 AS doubled",
	}}
	runtime := openRuntime(t, config)
	before := requireSnapshot(t, runtime, `[{"name":"initial","doubled":24}]`)
	t.Logf("initial bootstrap query output: sequence=%d rows=%s", before.Sequence, before.Rows[0].Value)
	output, err := exec.CommandContext(t.Context(), "docker", "exec", container, "psql",
		"-U", "postgres", "-d", "watchfixture", "-v", "ON_ERROR_STOP=1",
		"-c", "UPDATE watch_items SET amount = 18 WHERE id = 1").CombinedOutput()
	require.NoError(t, err, string(output))
	after := requireSnapshot(t, runtime, `[{"name":"initial","doubled":36}]`)
	require.Greater(t, after.Sequence, before.Sequence)
	t.Logf("SQL mutation query output: sequence=%d rows=%s", after.Sequence, after.Rows[0].Value)
}
