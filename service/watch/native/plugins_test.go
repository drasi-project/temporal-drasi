package native

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPluginCommandEncoding(t *testing.T) {
	t.Parallel()
	selection := PluginSelection{CatalogID: "trusted", PluginID: "source", Version: "1.0.0"}
	for _, test := range []struct {
		name     string
		command  Command
		expected string
	}{
		{"catalogs", ListCatalogsCommand{}, `{"op":"list_catalogs"}`},
		{"discover", DiscoverPluginsCommand{CatalogID: "trusted", Search: "postgres"}, `{"op":"discover_plugins","catalog_id":"trusted","search":"postgres"}`},
		{"inspect", InspectPluginCommand{Selection: selection}, `{"op":"inspect_plugin","selection":{"catalog_id":"trusted","plugin_id":"source","version":"1.0.0"}}`},
		{"install", InstallPluginCommand{Selection: selection, Digest: "sha256:pin"}, `{"op":"install_plugin","selection":{"catalog_id":"trusted","plugin_id":"source","version":"1.0.0"},"digest":"sha256:pin"}`},
		{"load", LoadPluginCommand{Digest: "sha256:pin"}, `{"op":"load_plugin","digest":"sha256:pin"}`},
		{"installations", ListPluginInstallationsCommand{}, `{"op":"list_plugin_installations"}`},
		{"installation", GetPluginInstallationCommand{Digest: "sha256:pin"}, `{"op":"get_plugin_installation","digest":"sha256:pin"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			payload, err := marshalCommand(test.command)
			require.NoError(t, err)
			require.JSONEq(t, test.expected, string(payload))
		})
	}
}

func TestPluginDescriptorDecodingPreservesSDKSchemaAndSupport(t *testing.T) {
	t.Parallel()
	var result Result
	require.NoError(t, json.Unmarshal([]byte(`{"plugins":{"sdk_abi":"0.14.0","target":"test-target","descriptors":[{"plugin":"source","kind":"postgres","category":"source","config_version":"1.0.0","schema_name":"source.Config","schema":{"source.Config":{"type":"object","x-ui":{"secret":true}}},"instantiable":true}]}}`), &result))
	var descriptors PluginDescriptors
	require.NoError(t, json.Unmarshal(result.Plugins, &descriptors))
	require.Equal(t, PluginDescriptors{
		SDKABI: "0.14.0", Target: "test-target",
		Descriptors: []PluginDescriptor{{
			Plugin: "source", Component: "postgres", Category: PluginSource,
			ConfigVersion: "1.0.0", SchemaName: "source.Config",
			ConfigurationSchema: json.RawMessage(`{"source.Config":{"type":"object","x-ui":{"secret":true}}}`),
			Supported:           true,
		}},
	}, descriptors)
}
