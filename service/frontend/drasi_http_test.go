package frontend

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type drasiTransportRequest struct {
	request proto.Message
	token   string
}

type drasiTransportWatches struct {
	contractpb.UnimplementedWatchServiceServer
	requests chan drasiTransportRequest
}

func (h *drasiTransportWatches) CreateSource(ctx context.Context, request *contractpb.CreateSourceRequest) (*contractpb.Source, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	h.requests <- drasiTransportRequest{request: request, token: md.Get("authorization")[0]}
	return &contractpb.Source{Namespace: request.Namespace, Id: request.Id, Revision: 1, Spec: request.Spec}, nil
}

type drasiTransportPlugins struct {
	contractpb.UnimplementedPluginServiceServer
	requests chan drasiTransportRequest
}

func (h *drasiTransportPlugins) InstallPlugin(ctx context.Context, request *contractpb.InstallPluginRequest) (*contractpb.PluginInstallation, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	h.requests <- drasiTransportRequest{request: request, token: md.Get("authorization")[0]}
	return &contractpb.PluginInstallation{Plugin: &contractpb.Plugin{
		CatalogId: request.CatalogId, Id: request.PluginId, Version: request.Version, Digest: request.Digest,
	}}, nil
}

func (h *drasiTransportPlugins) ListCatalogs(ctx context.Context, request *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	h.requests <- drasiTransportRequest{request: request, token: md.Get("authorization")[0]}
	return &contractpb.ListCatalogsResponse{}, nil
}

type drasiTransportNamespaceChecker struct{}

func (drasiTransportNamespaceChecker) Exists(name namespace.Name) error {
	if name != "example" {
		return serviceerror.NewNamespaceNotFound(name.String())
	}
	return nil
}

func TestDrasiTransportsPreserveAuthorizationAndValidation(t *testing.T) {
	t.Parallel()
	source := &contractpb.CreateSourceRequest{
		Namespace: "example", Id: "executions", RequestId: "create-source",
		Spec: &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}},
	}
	install := &contractpb.InstallPluginRequest{
		CatalogId: "trusted", PluginId: "source", Version: "1.0.0",
		Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RequestId: "install",
	}
	for _, transport := range []string{"http", "grpc"} {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name    string
				claims  *authorization.Claims
				request proto.Message
				code    codes.Code
			}{
				{"cluster reader discovery", &authorization.Claims{System: authorization.RoleReader}, &contractpb.ListCatalogsRequest{PageSize: 1}, codes.OK},
				{"namespace administrator cannot install", &authorization.Claims{Namespaces: map[string]authorization.Role{"example": authorization.RoleAdmin}}, install, codes.PermissionDenied},
				{"cluster writer cannot install", &authorization.Claims{System: authorization.RoleWriter}, install, codes.PermissionDenied},
				{"cluster administrator can install", &authorization.Claims{System: authorization.RoleAdmin}, install, codes.OK},
				{"namespace writer can create source", &authorization.Claims{Namespaces: map[string]authorization.Role{"example": authorization.RoleWriter}}, source, codes.OK},
				{"reader cannot create source", &authorization.Claims{System: authorization.RoleReader}, source, codes.PermissionDenied},
				{"other namespace cannot create source", &authorization.Claims{Namespaces: map[string]authorization.Role{"other": authorization.RoleWriter}}, source, codes.PermissionDenied},
				{"malformed installation is rejected", &authorization.Claims{System: authorization.RoleAdmin}, &contractpb.InstallPluginRequest{RequestId: "invalid"}, codes.InvalidArgument},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					controller := gomock.NewController(t)
					mapper := authorization.NewMockClaimMapper(controller)
					mapper.EXPECT().GetClaims(gomock.Any()).Return(tc.claims, nil)
					auth := authorization.NewInterceptor(
						mapper, authorization.NewDefaultAuthorizer(), metrics.NoopMetricsHandler, log.NewNoopLogger(),
						drasiTransportNamespaceChecker{}, nil, "", "",
						dynamicconfig.GetBoolPropertyFn(false), dynamicconfig.GetBoolPropertyFn(false),
						dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), dynamicconfig.GetBoolPropertyFn(false),
					)
					serviceErrors := interceptor.NewServiceErrorInterceptor(
						dynamicconfig.GetIntPropertyFn(4<<20), metrics.NoopMetricsHandler, log.NewNoopLogger(),
					)
					interceptors := []grpc.UnaryServerInterceptor{serviceErrors.Intercept, auth.Intercept}
					requests := make(chan drasiTransportRequest, 1)
					watches := validation.WrapWatchService(&drasiTransportWatches{requests: requests})
					plugins := validation.WrapPluginService(&drasiTransportPlugins{requests: requests})
					if transport == "grpc" {
						server := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors...))
						contractpb.RegisterWatchServiceServer(server, watches)
						contractpb.RegisterPluginServiceServer(server, plugins)
						listener := bufconn.Listen(1 << 20)
						done := make(chan error, 1)
						go func() { done <- server.Serve(listener) }()
						t.Cleanup(func() {
							server.Stop()
							require.NoError(t, listener.Close())
							require.NoError(t, <-done)
						})
						conn, err := grpc.NewClient("passthrough:///drasi-authorization",
							grpc.WithTransportCredentials(insecure.NewCredentials()),
							grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
						)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, conn.Close()) })
						ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "test-token")
						switch request := tc.request.(type) {
						case *contractpb.CreateSourceRequest:
							_, err = contractpb.NewWatchServiceClient(conn).CreateSource(ctx, request)
						case *contractpb.InstallPluginRequest:
							_, err = contractpb.NewPluginServiceClient(conn).InstallPlugin(ctx, request)
						case *contractpb.ListCatalogsRequest:
							_, err = contractpb.NewPluginServiceClient(conn).ListCatalogs(ctx, request)
						default:
							t.Fatalf("unexpected request %T", request)
						}
						require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
					} else {
						namespaces := namespace.NewMockRegistry(controller)
						namespaces.EXPECT().GetNamespace(namespace.Name("example")).Return(
							namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: "example"}, nil, "active"), nil,
						).AnyTimes()
						listener, err := net.Listen("tcp", "127.0.0.1:0")
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, listener.Close()) })
						server, err := NewHTTPAPIServer(
							&Config{
								KeepAliveMaxConnectionIdle: dynamicconfig.GetDurationPropertyFn(time.Minute),
								HTTPAllowedHosts:           func() *regexp.Regexp { return regexp.MustCompile(".*") },
							},
							config.RPC{}, listener, nil, &WorkflowHandler{}, &OperatorHandlerImpl{},
							interceptors, metrics.NoopMetricsHandler, mux.NewRouter(), namespaces, log.NewNoopLogger(),
							watches, plugins,
						)
						require.NoError(t, err)
						t.Cleanup(func() { require.NoError(t, server.listener.Close()) })
						method, path := http.MethodPost, "/api/v1/drasi/plugins/installations"
						switch tc.request.(type) {
						case *contractpb.CreateSourceRequest:
							path = "/api/v1/namespaces/example/drasi/sources"
						case *contractpb.ListCatalogsRequest:
							method, path = http.MethodGet, "/api/v1/drasi/plugins/catalogs?pageSize=1"
						case *contractpb.InstallPluginRequest:
						default:
							t.Fatalf("unexpected request %T", tc.request)
						}
						body, err := protojson.Marshal(tc.request)
						require.NoError(t, err)
						request := httptest.NewRequest(method, path, bytes.NewReader(body))
						request.Header.Set("Content-Type", "application/json")
						request.Header.Set("Authorization", "test-token")
						recorder := httptest.NewRecorder()
						server.serveHTTP(recorder, request)
						expected := http.StatusOK
						switch tc.code {
						case codes.PermissionDenied:
							expected = http.StatusForbidden
						case codes.InvalidArgument:
							expected = http.StatusBadRequest
						default:
						}
						require.Equal(t, expected, recorder.Code, recorder.Body.String())
					}
					if tc.code == codes.OK {
						called := await.Rcv(t, requests)
						require.Equal(t, "test-token", called.token)
						protorequire.ProtoEqual(t, tc.request, called.request)
					} else {
						select {
						case called := <-requests:
							t.Fatalf("refused request reached the handler: %v", called.request)
						default:
						}
					}
				})
			}
		})
	}
}
