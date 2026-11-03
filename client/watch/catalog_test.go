package watch

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/nettest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type catalogTestServer struct {
	watchservice.UnimplementedWatchServiceServer
	catalogs *contractpb.ListCatalogsResponse
	plugins  *contractpb.ListPluginsResponse
	plugin   *contractpb.Plugin
	wait     bool
	started  chan struct{}
	deadline chan time.Time
}

func (s *catalogTestServer) ready(ctx context.Context) error {
	if s.deadline != nil {
		deadline, _ := ctx.Deadline()
		s.deadline <- deadline
	}
	if !s.wait {
		return nil
	}
	s.started <- struct{}{}
	<-ctx.Done()
	return status.FromContextError(ctx.Err()).Err()
}

func TestPluginCatalogRegistryDeadline(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"discovery", "inspection"} {
		for _, callerBounded := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/caller bounded=%t", method, callerBounded), func(t *testing.T) {
				t.Parallel()
				server := &catalogTestServer{
					plugins:  &contractpb.ListPluginsResponse{Plugins: []*contractpb.Plugin{catalogPlugin()}},
					plugin:   catalogPlugin(),
					deadline: make(chan time.Time, 1),
				}
				observer, stop := catalogObserver(t, server)
				defer stop()
				ctx := t.Context()
				started := time.Now()
				budget := 30 * time.Second
				if callerBounded {
					budget = 10 * time.Second
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, budget)
					defer cancel()
				}
				var err error
				switch method {
				case "discovery":
					_, err = observer.ListPlugins(ctx, &contractpb.ListPluginsRequest{CatalogId: "trusted"})
				case "inspection":
					_, err = observer.GetPlugin(ctx, &contractpb.GetPluginRequest{
						CatalogId: "trusted", PluginId: "fixture", Version: "1.0.0",
					})
				default:
					t.Fatalf("unexpected catalog method %s", method)
				}
				require.NoError(t, err)
				require.WithinDuration(t, started.Add(budget), await.Rcv(t, server.deadline), time.Second)
				require.Empty(t, observer.pluginSlots)
			})
		}
	}
}

func (s *catalogTestServer) ListCatalogs(ctx context.Context, _ *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	return common.CloneProto(s.catalogs), nil
}

func (s *catalogTestServer) ListPlugins(ctx context.Context, _ *contractpb.ListPluginsRequest) (*contractpb.ListPluginsResponse, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	return common.CloneProto(s.plugins), nil
}

func (s *catalogTestServer) GetPlugin(ctx context.Context, _ *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	return common.CloneProto(s.plugin), nil
}

func catalogObserver(t *testing.T, server *catalogTestServer) (*Observer, func()) {
	t.Helper()
	listener := nettest.NewListener(nettest.NewPipe())
	rpcServer := grpc.NewServer()
	watchservice.RegisterWatchServiceServer(rpcServer, server)
	done := make(chan error, 1)
	go func() { done <- rpcServer.Serve(listener) }()
	conn := nettest.NewRPCFactory(listener).CreateWatchGRPCConnection("passthrough:///watch")
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	controller := gomock.NewController(t)
	monitor, resolver := membership.NewMockMonitor(controller), membership.NewMockServiceResolver(controller)
	monitor.EXPECT().GetResolver(primitives.WatchService).Return(resolver, nil)
	resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("z:7239"), membership.NewHostInfoFromAddress("a:7239"),
	})
	factory := common.NewMockRPCFactory(controller)
	factory.EXPECT().CreateWatchGRPCConnection("a:7239").Return(conn)
	return NewObserver(nil, monitor, factory), func() {
		rpcServer.Stop()
		require.NoError(t, await.Rcv(t, done))
	}
}

func catalogPlugin() *contractpb.Plugin {
	return &contractpb.Plugin{
		CatalogId: "trusted", Id: "fixture", Version: "1.0.0", Digest: "sha256:" + strings.Repeat("a", 64),
	}
}

func TestPluginCatalogRPCDoesNotRouteThroughNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		request  proto.Message
		server   *catalogTestServer
		expected proto.Message
		code     codes.Code
	}{
		{
			name: "catalogs", request: &contractpb.ListCatalogsRequest{},
			server:   &catalogTestServer{catalogs: &contractpb.ListCatalogsResponse{Catalogs: []*contractpb.Catalog{{Id: "trusted"}}}},
			expected: &contractpb.ListCatalogsResponse{Catalogs: []*contractpb.Catalog{{Id: "trusted"}}},
		},
		{
			name: "discovery", request: &contractpb.ListPluginsRequest{CatalogId: "trusted", Search: "fixture"},
			server:   &catalogTestServer{plugins: &contractpb.ListPluginsResponse{Plugins: []*contractpb.Plugin{catalogPlugin()}}},
			expected: &contractpb.ListPluginsResponse{Plugins: []*contractpb.Plugin{catalogPlugin()}},
		},
		{
			name: "inspection", request: &contractpb.GetPluginRequest{CatalogId: "trusted", PluginId: "fixture", Version: "1.0.0"},
			server: &catalogTestServer{plugin: catalogPlugin()}, expected: catalogPlugin(),
		},
		{
			name: "missing catalog identity", request: &contractpb.ListCatalogsRequest{},
			server: &catalogTestServer{catalogs: &contractpb.ListCatalogsResponse{Catalogs: []*contractpb.Catalog{nil}}}, code: codes.DataLoss,
		},
		{
			name: "duplicate catalog", request: &contractpb.ListCatalogsRequest{},
			server: &catalogTestServer{catalogs: &contractpb.ListCatalogsResponse{Catalogs: []*contractpb.Catalog{{Id: "a"}, {Id: "a"}}}},
			code:   codes.DataLoss,
		},
		{
			name: "too many catalogs for page", request: &contractpb.ListCatalogsRequest{PageSize: 1},
			server: &catalogTestServer{catalogs: &contractpb.ListCatalogsResponse{Catalogs: []*contractpb.Catalog{{Id: "a"}, {Id: "b"}}}},
			code:   codes.DataLoss,
		},
		{
			name: "empty continuation", request: &contractpb.ListPluginsRequest{},
			server: &catalogTestServer{plugins: &contractpb.ListPluginsResponse{NextPageToken: "cursor"}}, code: codes.DataLoss,
		},
		{
			name: "unselected catalog", request: &contractpb.ListPluginsRequest{CatalogId: "other"},
			server: &catalogTestServer{plugins: &contractpb.ListPluginsResponse{Plugins: []*contractpb.Plugin{catalogPlugin()}}}, code: codes.DataLoss,
		},
		{
			name: "unselected search", request: &contractpb.ListPluginsRequest{Search: "not-fixture"},
			server: &catalogTestServer{plugins: &contractpb.ListPluginsResponse{Plugins: []*contractpb.Plugin{catalogPlugin()}}}, code: codes.DataLoss,
		},
		{
			name: "duplicate plugin", request: &contractpb.ListPluginsRequest{},
			server: &catalogTestServer{plugins: &contractpb.ListPluginsResponse{Plugins: []*contractpb.Plugin{catalogPlugin(), catalogPlugin()}}},
			code:   codes.DataLoss,
		},
		{
			name: "wrong release", request: &contractpb.GetPluginRequest{CatalogId: "trusted", PluginId: "fixture", Version: "2.0.0"},
			server: &catalogTestServer{plugin: catalogPlugin()}, code: codes.DataLoss,
		},
		{
			name: "oversized metadata", request: &contractpb.GetPluginRequest{CatalogId: "trusted", PluginId: "fixture", Version: "1.0.0"},
			server: &catalogTestServer{plugin: &contractpb.Plugin{
				CatalogId: "trusted", Id: "fixture", Version: "1.0.0", Digest: catalogPlugin().Digest, Message: strings.Repeat("x", 64<<10),
			}}, code: codes.DataLoss,
		},
		{
			name: "bounded deadline", request: &contractpb.ListCatalogsRequest{},
			server: &catalogTestServer{wait: true, started: make(chan struct{}, 1)}, code: codes.DeadlineExceeded,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observer, stop := catalogObserver(t, tc.server)
			defer stop()
			var actual proto.Message
			var err error
			switch request := tc.request.(type) {
			case *contractpb.ListCatalogsRequest:
				actual, err = observer.ListCatalogs(t.Context(), request)
			case *contractpb.ListPluginsRequest:
				actual, err = observer.ListPlugins(t.Context(), request)
			case *contractpb.GetPluginRequest:
				actual, err = observer.GetPlugin(t.Context(), request)
			default:
				t.Fatalf("unexpected catalog request %T", request)
			}
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			if tc.code == codes.OK {
				protorequire.ProtoEqual(t, tc.expected, actual)
			} else {
				require.False(t, actual.ProtoReflect().IsValid())
			}
			require.Empty(t, observer.pluginSlots)
		})
	}
}

func TestPluginCatalogRequiresAvailableMembership(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)
	monitor, resolver := membership.NewMockMonitor(controller), membership.NewMockServiceResolver(controller)
	monitor.EXPECT().GetResolver(primitives.WatchService).Return(resolver, nil)
	resolver.EXPECT().AvailableMembers().Return(nil)
	observer := NewObserver(nil, monitor, nil)
	response, err := observer.ListCatalogs(t.Context(), &contractpb.ListCatalogsRequest{})
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	require.Nil(t, response)
	require.Empty(t, observer.pluginSlots)
}

func TestPluginCatalogCancellationReleasesRPCSlot(t *testing.T) {
	t.Parallel()
	server := &catalogTestServer{wait: true, started: make(chan struct{}, 1)}
	observer, stop := catalogObserver(t, server)
	defer stop()
	ctx, cancel := context.WithCancel(t.Context())
	completed := make(chan error, 1)
	go func() {
		_, err := observer.ListCatalogs(ctx, &contractpb.ListCatalogsRequest{})
		completed <- err
	}()
	await.Rcv(t, server.started)
	cancel()
	err := await.Rcv(t, completed)
	require.Equal(t, codes.Canceled, serviceerror.ToStatus(err).Code())
	require.Empty(t, observer.pluginSlots)
}

func TestPluginCatalogSharesObservationBudget(t *testing.T) {
	t.Parallel()
	observer := NewObserver(nil, nil, nil)
	for range cap(observer.pluginSlots) {
		observer.pluginSlots <- struct{}{}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	response, err := observer.ListCatalogs(ctx, &contractpb.ListCatalogsRequest{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, response)
	require.Len(t, observer.pluginSlots, cap(observer.pluginSlots))
}
