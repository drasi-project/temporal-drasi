//go:build temporal_with_drasi && cgo

package watch

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
)

func catalogRuntime(t *testing.T, policy PluginPolicy) *Service {
	t.Helper()
	runtime, err := native.Open(t.Context(), native.Config{
		StateDir: filepath.Join(t.TempDir(), "catalog"), InstanceID: "catalog", OwnerEpoch: 1,
		StartMode: native.StartModeDormant, Catalogs: policy.Catalogs, Trust: policy.Trust, ManagedPlugins: policy.ManagedPlugins,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, runtime.Close(ctx))
	})
	return &Service{
		bootID: "00000000-0000-4000-8000-000000000001", params: ServiceParams{Logger: log.NewNoopLogger()},
		plugins: &runtimePlugins{runtime: runtime},
	}
}

func TestPluginCatalogUsesActualManagementRuntime(t *testing.T) {
	t.Parallel()
	service := catalogRuntime(t, PluginPolicy{Catalogs: []native.CatalogConfig{
		{ID: "z", DisplayName: "Last", Plugins: []native.CatalogRelease{}},
		{ID: "a", DisplayName: "First", Plugins: []native.CatalogRelease{}},
	}})
	first, err := service.ListCatalogs(t.Context(), &contractpb.ListCatalogsRequest{PageSize: 1})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &contractpb.ListCatalogsResponse{
		Catalogs:      []*contractpb.Catalog{{Id: "a", DisplayName: "First"}},
		NextPageToken: pluginPageToken(service.bootID, pluginPageScope("catalogs", "", ""), 1),
	}, first)
	last, err := service.ListCatalogs(t.Context(), &contractpb.ListCatalogsRequest{PageSize: 1, PageToken: first.NextPageToken})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &contractpb.ListCatalogsResponse{Catalogs: []*contractpb.Catalog{{Id: "z", DisplayName: "Last"}}}, last)
	discovery, err := service.ListPlugins(t.Context(), &contractpb.ListPluginsRequest{CatalogId: "a"})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &contractpb.ListPluginsResponse{}, discovery)
	_, err = service.ListPlugins(t.Context(), &contractpb.ListPluginsRequest{CatalogId: "unconfigured"})
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	_, err = service.GetPlugin(t.Context(), &contractpb.GetPluginRequest{CatalogId: "a", PluginId: "absent", Version: "1.0.0"})
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	observed, err := service.plugins.runtime.Apply(t.Context(), native.ObserveNodesCommand{})
	require.NoError(t, err)
	require.Empty(t, observed.Nodes)
	require.Nil(t, service.namespaces)
	service.pluginClosing = true
	_, err = service.ListCatalogs(t.Context(), &contractpb.ListCatalogsRequest{})
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
}

func TestPluginCatalogCancellationRetainsActualCompletion(t *testing.T) {
	t.Parallel()
	policy, started, release := blockedRuntimePluginPolicy(t)
	service := catalogRuntime(t, policy)
	t.Cleanup(release)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := service.GetPlugin(ctx, &contractpb.GetPluginRequest{
			CatalogId: "trusted", PluginId: "fixture-source", Version: "1.0.0",
		})
		result <- err
	}()
	await.Rcv(t, started)
	cancel()
	require.ErrorIs(t, await.Rcv(t, result), context.Canceled)
	_, err := service.ListCatalogs(t.Context(), &contractpb.ListCatalogsRequest{})
	require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
	observed, err := service.plugins.runtime.Apply(t.Context(), native.ObserveNodesCommand{})
	require.NoError(t, err)
	require.Empty(t, observed.Nodes)
	closed := make(chan error, 1)
	go func() { closed <- service.plugins.runtime.Close(t.Context()) }()
	await.Require(t.Context(), t, func(t *await.T) {
		_, err := service.plugins.runtime.Plugin(native.ListCatalogsCommand{})
		require.ErrorContains(t, err, "sealed")
	}, 5*time.Second, 10*time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("close passed actual blocked catalog work: %v", err)
	default:
	}
	release()
	require.NoError(t, await.Rcv(t, closed))
}
