//go:build integration && temporal_with_drasi && cgo

package temporal_test

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/chasm/lib/watchregistry"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
)

func installWatchRolePluginIntent(t *testing.T, history *grpc.ClientConn) *registrypb.ReadClusterPluginsResponse {
	t.Helper()
	client := registrypb.NewClusterPluginServiceClient(history)
	plugin := &contractpb.Plugin{
		CatalogId: "role-fixture", Id: "role-observation-source", Version: "1.0.0", Digest: "sha256:" + strings.Repeat("a", 64),
	}
	_, err := client.InstallClusterPlugin(t.Context(), &registrypb.InstallClusterPluginRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: watchregistry.ClusterPluginBusinessID,
		Request: &contractpb.InstallPluginRequest{
			CatalogId: plugin.CatalogId, PluginId: plugin.Id, Version: plugin.Version, Digest: plugin.Digest,
			RequestId: "role-plugin-install",
		},
		Resolution: &registrypb.InstallClusterPluginRequest_Plugin{Plugin: plugin},
	})
	require.NoError(t, err)
	_, err = client.LoadClusterPlugin(t.Context(), &registrypb.LoadClusterPluginRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: watchregistry.ClusterPluginBusinessID,
		Request: &contractpb.LoadPluginRequest{Digest: plugin.Digest, RequestId: "role-plugin-load"},
	})
	require.NoError(t, err)
	snapshot, err := client.ReadClusterPlugins(t.Context(), &registrypb.ReadClusterPluginsRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: watchregistry.ClusterPluginBusinessID,
		Operation: &registrypb.ReadClusterPluginsRequest_Snapshot{Snapshot: true},
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{
		Generation: 2, Plugins: []*contractpb.Plugin{plugin}, DesiredLoads: map[string]string{plugin.Id: plugin.Digest},
	}, snapshot)
	return snapshot
}

func requireWatchRolePublicPluginObservation(t *testing.T, conn *grpc.ClientConn, snapshot *registrypb.ReadClusterPluginsResponse, httpPort int) {
	t.Helper()
	client := contractpb.NewPluginServiceClient(conn)
	plugin := snapshot.Plugins[0]
	await.Require(t.Context(), t, func(t *await.T) {
		capabilities, err := client.GetCapabilities(t.Context(), &contractpb.GetPluginCapabilitiesRequest{})
		require.NoError(t, err)
		require.True(t, capabilities.Enabled)
		require.Contains(t, capabilities.AllowedMethods, contractpb.PluginService_InstallPlugin_FullMethodName)
		catalogs, err := client.ListCatalogs(t.Context(), &contractpb.ListCatalogsRequest{})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &contractpb.ListCatalogsResponse{}, catalogs)
		installation, err := client.GetInstallation(t.Context(), &contractpb.GetInstallationRequest{Digest: plugin.Digest})
		require.NoError(t, err)
		require.True(t, installation.LoadRequested)
		protorequire.ProtoEqual(t, plugin, installation.Plugin)
		require.Len(t, installation.Hosts, 1)
		require.Equal(t, contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED, installation.Hosts[0].State)
		require.Empty(t, installation.Hosts[0].VerifiedSignerIdentity)
		replayed, err := client.InstallPlugin(t.Context(), &contractpb.InstallPluginRequest{
			CatalogId: plugin.CatalogId, PluginId: plugin.Id, Version: plugin.Version,
			Digest: plugin.Digest, RequestId: "role-plugin-install",
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, installation, replayed)
		loaded, err := client.LoadPlugin(t.Context(), &contractpb.LoadPluginRequest{
			Digest: plugin.Digest, RequestId: "role-plugin-load",
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, installation, loaded)
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
			fmt.Sprintf("http://127.0.0.1:%d/api/v1/drasi/plugins/installations/%s", httpPort, url.PathEscape(plugin.Digest)), nil)
		require.NoError(t, err)
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer func() { require.NoError(t, response.Body.Close()) }()
		require.Equal(t, http.StatusOK, response.StatusCode)
		body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		require.NoError(t, err)
		require.LessOrEqual(t, len(body), 1<<20)
		var httpInstallation contractpb.PluginInstallation
		require.NoError(t, protojson.Unmarshal(body, &httpInstallation))
		protorequire.ProtoEqual(t, installation, &httpInstallation)
	}, 30*time.Second, 100*time.Millisecond)
}

func requireWatchRolePluginObservation(t *testing.T, conn *grpc.ClientConn, snapshot *registrypb.ReadClusterPluginsResponse, hostID, bootID string) {
	t.Helper()
	client := watchservice.NewWatchServiceClient(conn)
	await.Require(t.Context(), t, func(t *await.T) {
		catalogs, err := client.ListCatalogs(t.Context(), &contractpb.ListCatalogsRequest{})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &contractpb.ListCatalogsResponse{}, catalogs)
		plugins, err := client.ListPlugins(t.Context(), &contractpb.ListPluginsRequest{})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &contractpb.ListPluginsResponse{}, plugins)
		_, err = client.GetPlugin(t.Context(), &contractpb.GetPluginRequest{
			CatalogId: snapshot.Plugins[0].CatalogId, PluginId: snapshot.Plugins[0].Id, Version: snapshot.Plugins[0].Version,
		})
		require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
		response, err := client.ObservePlugins(t.Context(), &watchservice.ObservePluginsRequest{
			Generation: snapshot.Generation, Digests: []string{snapshot.Plugins[0].Digest},
		})
		require.NoError(t, err)
		protorequire.ProtoEqual(t, &watchservice.ObservePluginsResponse{
			Generation: snapshot.Generation, StorageHostId: hostID, ProcessBootId: bootID,
			Observations: []*watchservice.PluginObservation{{
				Digest: snapshot.Plugins[0].Digest,
				Status: &contractpb.PluginHostStatus{
					HostId: hostID, Message: "One or more local runtimes have no fresh plugin observation",
				},
			}},
		}, response)
	}, 30*time.Second, 100*time.Millisecond)
}
