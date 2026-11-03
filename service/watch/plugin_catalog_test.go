package watch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func discoveredCatalogPlugin() native.DiscoveredPlugin {
	return native.DiscoveredPlugin{
		CatalogID: "trusted", ID: "fixture", Version: "1.0.0", Digest: "sha256:" + strings.Repeat("a", 64),
		Platform: "target", ABIVersion: "abi", SDKVersion: "sdk", CoreVersion: "core", Compatible: true,
		Descriptors: []native.PluginDescriptor{{
			Plugin: "fixture", Component: "fixture-source", Category: native.PluginSource, Supported: true,
			SchemaName: "Config", ConfigurationSchema: json.RawMessage(`{"Config":{"type":"object"}}`),
		}},
	}
}

func TestPluginCatalogMetadataProjection(t *testing.T) {
	t.Parallel()
	schema, err := structpb.NewStruct(map[string]any{
		"type": "object",
		"components": map[string]any{
			"schemas": map[string]any{"Config": map[string]any{"type": "object"}},
		},
	})
	require.NoError(t, err)
	expected := &contractpb.Plugin{
		CatalogId: "trusted", Id: "fixture", Version: "1.0.0", Digest: "sha256:" + strings.Repeat("a", 64),
		Platform: "target", AbiVersion: "abi", SdkVersion: "sdk", CoreVersion: "core", Compatible: true,
		Descriptors: []*contractpb.PluginDescriptor{{
			Component: "fixture-source", Category: contractpb.PluginCategory_PLUGIN_CATEGORY_SOURCE,
			ConfigurationSchema: schema, Supported: true,
		}},
	}
	plugin, err := publicDiscoveredPlugin(discoveredCatalogPlugin())
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, plugin)
	for _, tc := range []struct {
		name   string
		change func(*native.DiscoveredPlugin)
		code   codes.Code
	}{
		{"digest", func(p *native.DiscoveredPlugin) { p.Digest = "sha256:invalid" }, codes.DataLoss},
		{"catalog URL", func(p *native.DiscoveredPlugin) { p.CatalogID = "https://untrusted.example" }, codes.DataLoss},
		{"wrong descriptor owner", func(p *native.DiscoveredPlugin) { p.Descriptors[0].Plugin = "another-plugin" }, codes.DataLoss},
		{"unknown category", func(p *native.DiscoveredPlugin) { p.Descriptors[0].Category = "unknown" }, codes.DataLoss},
		{"invalid component", func(p *native.DiscoveredPlugin) { p.Descriptors[0].Component = "../path" }, codes.DataLoss},
		{"duplicate descriptor", func(p *native.DiscoveredPlugin) {
			p.Descriptors = append(p.Descriptors, p.Descriptors[0])
		}, codes.DataLoss},
		{"unknown schema shape", func(p *native.DiscoveredPlugin) {
			p.Descriptors[0].ConfigurationSchema = json.RawMessage(`[]`)
		}, codes.DataLoss},
		{"null schema", func(p *native.DiscoveredPlugin) {
			p.Descriptors[0].ConfigurationSchema = json.RawMessage(`null`)
		}, codes.DataLoss},
		{"missing schema name", func(p *native.DiscoveredPlugin) {
			p.Descriptors[0].SchemaName = ""
		}, codes.DataLoss},
		{"unknown schema name", func(p *native.DiscoveredPlugin) {
			p.Descriptors[0].SchemaName = "Other"
		}, codes.DataLoss},
		{"non-object root schema", func(p *native.DiscoveredPlugin) {
			p.Descriptors[0].ConfigurationSchema = json.RawMessage(`{"Config":[]}`)
		}, codes.DataLoss},
		{"conflicting schema references", func(p *native.DiscoveredPlugin) {
			p.Descriptors[0].ConfigurationSchema = json.RawMessage(`{"Config":{"components":{}}}`)
		}, codes.DataLoss},
		{"oversized schema", func(p *native.DiscoveredPlugin) {
			p.Descriptors[0].ConfigurationSchema = json.RawMessage(`{"description":"` + strings.Repeat("x", 64<<10) + `"}`)
		}, codes.DataLoss},
		{"unsupported reaction", func(p *native.DiscoveredPlugin) {
			p.Descriptors[0].Category = native.PluginReaction
		}, codes.DataLoss},
		{"too many descriptors", func(p *native.DiscoveredPlugin) {
			p.Descriptors = make([]native.PluginDescriptor, 129)
		}, codes.DataLoss},
		{"invalid text", func(p *native.DiscoveredPlugin) { p.Message = "\xff" }, codes.DataLoss},
		{"oversized metadata", func(p *native.DiscoveredPlugin) { p.Message = strings.Repeat("x", 64<<10) }, codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			discovered := discoveredCatalogPlugin()
			tc.change(&discovered)
			plugin, err := publicDiscoveredPlugin(discovered)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			require.Nil(t, plugin)
		})
	}
}

func TestPluginCatalogSchemaPreservesLocalReferences(t *testing.T) {
	t.Parallel()
	discovered := discoveredCatalogPlugin()
	discovered.Descriptors[0].ConfigurationSchema = json.RawMessage(`{
		"Config":{"type":"object","properties":{"port":{"$ref":"#/components/schemas/Port"}},"required":["port"]},
		"Port":{"type":"integer","minimum":1}
	}`)
	expected := &structpb.Struct{}
	require.NoError(t, expected.UnmarshalJSON([]byte(`{
		"type":"object","properties":{"port":{"$ref":"#/components/schemas/Port"}},"required":["port"],
		"components":{"schemas":{
			"Config":{"type":"object","properties":{"port":{"$ref":"#/components/schemas/Port"}},"required":["port"]},
			"Port":{"type":"integer","minimum":1}
		}}
	}`)))
	plugin, err := publicDiscoveredPlugin(discovered)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, plugin.Descriptors[0].ConfigurationSchema)
	require.JSONEq(t, `{
		"Config":{"type":"object","properties":{"port":{"$ref":"#/components/schemas/Port"}},"required":["port"]},
		"Port":{"type":"integer","minimum":1}
	}`, string(discovered.Descriptors[0].ConfigurationSchema))
}

func TestPluginCatalogTokensFenceHostAndFilters(t *testing.T) {
	t.Parallel()
	const boot = "00000000-0000-4000-8000-000000000001"
	scope := pluginPageScope("plugins", "trusted", "fixture")
	token := pluginPageToken(boot, scope, 3)
	offset, err := pluginPageOffset(token, boot, scope)
	require.NoError(t, err)
	require.Equal(t, uint32(3), offset)
	for _, tc := range []struct {
		name  string
		token string
		boot  string
		scope string
		code  codes.Code
	}{
		{"malformed", "*", boot, scope, codes.InvalidArgument},
		{"shape", base64.RawURLEncoding.EncodeToString([]byte("1:missing")), boot, scope, codes.InvalidArgument},
		{"zero offset", pluginPageToken(boot, scope, 0), boot, scope, codes.InvalidArgument},
		{"noncanonical offset", base64.RawURLEncoding.EncodeToString([]byte("1:" + boot + ":" + scope + ":03")), boot, scope, codes.InvalidArgument},
		{"overflow", base64.RawURLEncoding.EncodeToString([]byte("1:" + boot + ":" + scope + ":4294967296")), boot, scope, codes.InvalidArgument},
		{"successor process", token, "00000000-0000-4000-8000-000000000002", scope, codes.FailedPrecondition},
		{"another method", token, boot, pluginPageScope("catalogs", "", ""), codes.FailedPrecondition},
		{"another catalog", token, boot, pluginPageScope("plugins", "other", "fixture"), codes.FailedPrecondition},
		{"another search", token, boot, pluginPageScope("plugins", "trusted", ""), codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := pluginPageOffset(tc.token, tc.boot, tc.scope)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
		})
	}
}

func TestPluginCatalogRequestBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		request proto.Message
		code    codes.Code
	}{
		{"missing catalogs request", (*contractpb.ListCatalogsRequest)(nil), codes.InvalidArgument},
		{"catalog page limit", &contractpb.ListCatalogsRequest{PageSize: 201}, codes.InvalidArgument},
		{"malformed token", &contractpb.ListCatalogsRequest{PageToken: "invalid"}, codes.InvalidArgument},
		{"missing plugins request", (*contractpb.ListPluginsRequest)(nil), codes.InvalidArgument},
		{"arbitrary registry", &contractpb.ListPluginsRequest{CatalogId: "attacker.example/plugins"}, codes.InvalidArgument},
		{"search limit", &contractpb.ListPluginsRequest{Search: strings.Repeat("x", 257)}, codes.InvalidArgument},
		{"missing inspection", (*contractpb.GetPluginRequest)(nil), codes.InvalidArgument},
		{"incomplete inspection", &contractpb.GetPluginRequest{}, codes.InvalidArgument},
		{"unavailable runtime", &contractpb.ListCatalogsRequest{}, codes.Unavailable},
		{"unavailable discovery", &contractpb.ListPluginsRequest{}, codes.Unavailable},
		{"unavailable inspection", &contractpb.GetPluginRequest{CatalogId: "trusted", PluginId: "fixture", Version: "1.0.0"}, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			service := &Service{}
			var err error
			switch request := tc.request.(type) {
			case *contractpb.ListCatalogsRequest:
				_, err = service.ListCatalogs(t.Context(), request)
			case *contractpb.ListPluginsRequest:
				_, err = service.ListPlugins(t.Context(), request)
			case *contractpb.GetPluginRequest:
				_, err = service.GetPlugin(t.Context(), request)
			default:
				t.Fatalf("unexpected catalog request %T", request)
			}
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := (&Service{}).ListCatalogs(ctx, &contractpb.ListCatalogsRequest{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestPluginCatalogPageReservesContinuationBytes(t *testing.T) {
	t.Parallel()
	result := native.Result{MorePlugins: true}
	for i := range 16 {
		plugin := discoveredCatalogPlugin()
		plugin.ID = fmt.Sprintf("fixture-%02d", i)
		plugin.Descriptors[0].Plugin = plugin.ID
		public, err := publicDiscoveredPlugin(plugin)
		require.NoError(t, err)
		plugin.Message = strings.Repeat("x", (64<<10)-proto.Size(public)-8)
		public, err = publicDiscoveredPlugin(plugin)
		require.NoError(t, err)
		require.LessOrEqual(t, proto.Size(public), 64<<10)
		result.DiscoveredPlugins = append(result.DiscoveredPlugins, plugin)
	}
	page, more, err := projectPluginPage(&contractpb.ListPluginsRequest{}, result)
	require.NoError(t, err)
	require.True(t, more)
	require.Len(t, page.Plugins, 15)
	page.NextPageToken = strings.Repeat("x", 4096)
	require.LessOrEqual(t, proto.Size(page), validation.MaxRequestBytes)
	for i, plugin := range page.Plugins {
		require.Equal(t, result.DiscoveredPlugins[i].ID, plugin.Id)
	}
	tail, more, err := projectPluginPage(&contractpb.ListPluginsRequest{}, native.Result{
		DiscoveredPlugins: result.DiscoveredPlugins[len(page.Plugins):],
	})
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, tail.Plugins, 1)
	require.Equal(t, result.DiscoveredPlugins[15].ID, tail.Plugins[0].Id)
}
