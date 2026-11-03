package validation_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	watchpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestPublicMethodPolicyAndRoutes(t *testing.T) {
	t.Parallel()
	services := watchpb.File_drasi_watch_v1_service_proto.Services()
	for i := 0; i < services.Len(); i++ {
		service := services.Get(i)
		for j := 0; j < service.Methods().Len(); j++ {
			method := service.Methods().Get(j)
			t.Run(string(service.Name())+"/"+string(method.Name()), func(t *testing.T) {
				t.Parallel()
				fullMethod := "/" + string(service.FullName()) + "/" + string(method.Name())
				policy, ok := validation.Policy(fullMethod)
				require.True(t, ok)
				access := proto.GetExtension(method.Options(), watchpb.E_Access).(watchpb.Access)
				require.Equal(t, access, policy.Access)
				require.NotEqual(t, watchpb.Access_ACCESS_UNSPECIFIED, policy.Access)
				rule := proto.GetExtension(method.Options(), annotations.E_Http).(*annotations.HttpRule)
				path := rule.GetGet()
				if path == "" {
					path = rule.GetPost()
				}
				if path == "" {
					path = rule.GetPut()
				}
				if path == "" {
					path = rule.GetDelete()
				}
				if service.Name() == "WatchService" {
					require.True(t, strings.HasPrefix(path, "/api/v1/namespaces/{namespace}/drasi/"))
					require.NotNil(t, method.Input().Fields().ByName("namespace"))
				} else {
					require.True(t, strings.HasPrefix(path, "/api/v1/drasi/plugins"))
					require.Nil(t, method.Input().Fields().ByName("namespace"))
				}
				expectedMutation := access == watchpb.Access_ACCESS_NAMESPACE_WRITE || access == watchpb.Access_ACCESS_CLUSTER_ADMIN
				require.Equal(t, expectedMutation, policy.Mutation)
				if policy.Mutation {
					require.NotNil(t, method.Input().Fields().ByName("request_id"))
				}
			})
		}
	}
}

func TestCapabilitiesAuthorizationScopes(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		fullMethod string
		request    proto.Message
		access     watchpb.Access
		namespace  string
	}{
		{
			name: "cluster plugins", fullMethod: watchpb.PluginService_GetCapabilities_FullMethodName,
			request: &watchpb.GetPluginCapabilitiesRequest{}, access: watchpb.Access_ACCESS_CLUSTER_READ,
		},
		{
			name: "namespace watches", fullMethod: watchpb.WatchService_GetCapabilities_FullMethodName,
			request: &watchpb.GetCapabilitiesRequest{Namespace: "test"},
			access:  watchpb.Access_ACCESS_NAMESPACE_READ, namespace: "test",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			policy, ok := validation.Policy(test.fullMethod)
			require.True(t, ok)
			require.Equal(t, validation.MethodPolicy{Access: test.access}, policy)
			require.Equal(t, test.namespace, validation.Namespace(test.request))
			require.NoError(t, validation.ValidateRequest(test.request))
		})
	}
}

func TestSecretReferenceImmutableVersionRoundTrip(t *testing.T) {
	t.Parallel()
	for _, wire := range []string{
		`{"provider":"databases","key":"replication"}`,
		`{"provider":"databases","key":"replication","version":"generation/17:rotation"}`,
	} {
		t.Run(wire, func(t *testing.T) {
			t.Parallel()
			reference := &watchpb.SecretReference{}
			require.NoError(t, protojson.Unmarshal([]byte(wire), reference))
			encoded, err := proto.Marshal(reference)
			require.NoError(t, err)
			restored := &watchpb.SecretReference{}
			require.NoError(t, proto.Unmarshal(encoded, restored))
			actual, err := protojson.Marshal(restored)
			require.NoError(t, err)
			require.JSONEq(t, wire, string(actual))
		})
	}
}

func TestGRPCAndGatewayRegistration(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"direct", "grpc"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			mux := runtime.NewServeMux()
			expectedCapabilities := &watchpb.PluginCapabilities{
				ApiVersion: "v1", MaxRequestBytes: validation.MaxRequestBytes,
				AllowedMethods: []string{watchpb.PluginService_GetCapabilities_FullMethodName},
				Message:        "Native plugin management is disabled",
			}
			plugins := &pluginServer{capabilities: expectedCapabilities}
			if mode == "direct" {
				require.NoError(t, validation.RegisterWatchServiceHandlerServer(ctx, mux, &watchServer{}))
				require.NoError(t, validation.RegisterPluginServiceHandlerServer(ctx, mux, plugins))
			} else {
				listener := bufconn.Listen(1 << 20)
				server := grpc.NewServer(grpc.UnaryInterceptor(validation.UnaryServerInterceptor))
				watchpb.RegisterWatchServiceServer(server, &watchServer{})
				watchpb.RegisterPluginServiceServer(server, plugins)
				done := make(chan error, 1)
				go func() { done <- server.Serve(listener) }()
				t.Cleanup(func() {
					server.Stop()
					require.NoError(t, listener.Close())
					require.NoError(t, <-done)
				})
				connection, err := grpc.NewClient("passthrough:///watch-contract",
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
						return listener.DialContext(ctx)
					}))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, connection.Close()) })
				require.NoError(t, watchpb.RegisterWatchServiceHandler(ctx, mux, connection))
				require.NoError(t, watchpb.RegisterPluginServiceHandler(ctx, mux, connection))
				_, err = watchpb.NewWatchServiceClient(connection).CreateWatch(ctx, &watchpb.CreateWatchRequest{Namespace: "test"})
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				capabilities, err := watchpb.NewPluginServiceClient(connection).GetCapabilities(ctx, &watchpb.GetPluginCapabilitiesRequest{})
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedCapabilities, capabilities))
			}

			recorder := httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/drasi/plugins/capabilities", nil))
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			capabilities := &watchpb.PluginCapabilities{}
			require.NoError(t, protojson.Unmarshal(recorder.Body.Bytes(), capabilities))
			require.True(t, proto.Equal(expectedCapabilities, capabilities))

			request := &watchpb.CreateWatchRequest{Namespace: "test", Id: "watch", RequestId: "create", Spec: watchSpec()}
			body, err := protojson.Marshal(request)
			require.NoError(t, err)
			recorder = httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/test/drasi/watches", strings.NewReader(string(body))))
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			response := &watchpb.Watch{}
			require.NoError(t, protojson.Unmarshal(recorder.Body.Bytes(), response))
			require.True(t, proto.Equal(&watchpb.Watch{
				Namespace: "test", Id: "watch", Revision: 1, Spec: request.Spec,
				DesiredState: watchpb.DesiredState_DESIRED_STATE_RUNNING,
			}, response))

			sourceRequest := &watchpb.CreateSourceRequest{
				Namespace: "test", Id: "source", RequestId: "source-create", Spec: externalDefinition(),
			}
			sourceRequest.Spec.Bootstrap = proto.Clone(sourceRequest.Spec.GetExternal()).(*watchpb.ComponentConfiguration)
			sourceRequest.Spec.Bootstrap.Secrets["connection"].Version = "bootstrap-19"
			sourceBody, err := protojson.Marshal(sourceRequest)
			require.NoError(t, err)
			recorder = httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/test/drasi/sources", strings.NewReader(string(sourceBody))))
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			source := &watchpb.Source{}
			require.NoError(t, protojson.Unmarshal(recorder.Body.Bytes(), source))
			require.True(t, proto.Equal(&watchpb.Source{
				Namespace: "test", Id: "source", Revision: 1, Spec: sourceRequest.Spec,
				DesiredState: watchpb.DesiredState_DESIRED_STATE_STOPPED,
			}, source))
			sourceRequest.Spec.GetExternal().Secrets["connection"].Version = ""
			sourceBody, err = protojson.Marshal(sourceRequest)
			require.NoError(t, err)
			recorder = httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/test/drasi/sources", strings.NewReader(string(sourceBody))))
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())

			for _, invalidBody := range []string{
				`{"id":"watch","requestId":"create"}`,
				`{"id":"watch","requestId":"create","spec":{"action":{"start":{},"signal":{}}}}`,
			} {
				recorder := httptest.NewRecorder()
				mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/namespaces/test/drasi/watches", strings.NewReader(invalidBody)))
				require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			}

			recorder = httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/drasi/plugins/catalogs", nil))
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.JSONEq(t, `{"catalogs":[{"id":"trusted","displayName":"Trusted catalog"}],"nextPageToken":""}`, recorder.Body.String())

			recorder = httptest.NewRecorder()
			mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/drasi/plugins/installations", strings.NewReader(`{"catalogId":"trusted"}`)))
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

type watchServer struct {
	watchpb.UnimplementedWatchServiceServer
}

func (*watchServer) CreateSource(_ context.Context, request *watchpb.CreateSourceRequest) (*watchpb.Source, error) {
	if err := validation.ValidateSourceDefinition(request.Spec); err != nil {
		return nil, err
	}
	return &watchpb.Source{
		Namespace: request.Namespace, Id: request.Id, Revision: 1, Spec: request.Spec,
		DesiredState: watchpb.DesiredState_DESIRED_STATE_STOPPED,
	}, nil
}

func (*watchServer) CreateWatch(_ context.Context, request *watchpb.CreateWatchRequest) (*watchpb.Watch, error) {
	return &watchpb.Watch{
		Namespace: request.Namespace, Id: request.Id, Revision: 1, Spec: request.Spec,
		DesiredState: watchpb.DesiredState_DESIRED_STATE_RUNNING,
	}, nil
}

type pluginServer struct {
	watchpb.UnimplementedPluginServiceServer
	capabilities *watchpb.PluginCapabilities
}

func (server *pluginServer) GetCapabilities(context.Context, *watchpb.GetPluginCapabilitiesRequest) (*watchpb.PluginCapabilities, error) {
	return server.capabilities, nil
}

func (*pluginServer) ListCatalogs(context.Context, *watchpb.ListCatalogsRequest) (*watchpb.ListCatalogsResponse, error) {
	return &watchpb.ListCatalogsResponse{Catalogs: []*watchpb.Catalog{{Id: "trusted", DisplayName: "Trusted catalog"}}}, nil
}
