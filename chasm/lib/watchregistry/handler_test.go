package watchregistry

import (
	"context"
	"net"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRegistryNamespaceEnablementAndCleanup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		global  bool
		enabled bool
		message string
	}{
		{"default off", false, false, "disabled"},
		{"global namespace", true, true, "global namespaces"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t, DefaultLimits())
			_, err := env.mutate(env.createSource("source", "accepted"))
			require.NoError(t, err)
			ns := namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Id: env.id, Name: env.namespace}, nil, tc.global, nil, 0)
			namespaces := namespace.NewMockRegistry(gomock.NewController(t))
			namespaces.EXPECT().GetNamespaceByID(namespace.ID(env.id)).Return(ns, nil).AnyTimes()
			env.handler.namespaces, env.enabled = namespaces, tc.enabled
			_, err = env.mutate(env.createSource("new", "disabled"))
			require.ErrorContains(t, err, tc.message)
			_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StopSource{
				StopSource: env.sourceOperation("source", 1, "stop"),
			}})
			require.NoError(t, err)
			_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{
				DeleteSource: env.sourceOperation("source", 2, "delete"),
			}})
			require.NoError(t, err)
			response, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_GetSource{
				GetSource: &contractpb.GetSourceRequest{Namespace: env.namespace, Id: "source"},
			}})
			require.NoError(t, err)
			require.Equal(t, registrypb.DESIRED_STATE_DELETED, response.Records[0].Desired)
		})
	}
}

func TestRegistryUsesDefaultOffDynamicConfig(t *testing.T) {
	t.Parallel()
	h, err := newHandler(nil, dynamicconfig.NewNoopCollection(), DefaultLimits())
	require.NoError(t, err)
	require.False(t, h.enabled(testvars.New(t).NamespaceName().String()))
}

func TestRegistryBoundaryValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*registrypb.MutateRequest)
		text   string
	}{
		{"namespace mismatch", func(r *registrypb.MutateRequest) { r.GetCreateSource().Namespace += "-other" }, "authoritative namespace"},
		{"namespace slash", func(r *registrypb.MutateRequest) { r.GetCreateSource().Namespace += "/other" }, "must not contain a slash"},
		{"missing specification", func(r *registrypb.MutateRequest) { r.GetCreateSource().Spec = nil }, "spec"},
		{"missing temporal variant", func(r *registrypb.MutateRequest) {
			r.GetCreateSource().Spec.Kind = &contractpb.SourceSpec_Temporal{}
		}, "spec.temporal"},
		{"missing request identity", func(r *registrypb.MutateRequest) { r.GetCreateSource().RequestId = "" }, "request_id"},
		{"missing operation", func(r *registrypb.MutateRequest) { r.Operation = nil }, "operation"},
		{"missing operation body", func(r *registrypb.MutateRequest) {
			r.Operation = &registrypb.MutateRequest_CreateSource{}
		}, "request is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t, DefaultLimits())
			request := env.createSource("source", "request")
			tc.change(request)
			_, err := env.mutate(request)
			require.ErrorContains(t, err, tc.text)
		})
	}
	for _, tc := range []struct {
		name    string
		request keyedRequest
	}{
		{"nil", nil},
		{"typed nil", (*registrypb.MutateRequest)(nil)},
		{"invalid namespace", &registrypb.ReadRequest{NamespaceId: "not-a-uuid", ExecutionId: BusinessID}},
		{"mutable business ID", &registrypb.ReadRequest{NamespaceId: testvars.New(t).NamespaceID().String(), ExecutionId: "other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := (&handler{}).resolve(tc.request)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
		})
	}
}

func TestRegistryPrivateGRPC(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(
		func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			return next(chasm.NewEngineContext(ctx, env.engine), request)
		},
	))
	(&Library{handler: env.handler}).RegisterServices(server)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, listener.Close())
		require.NoError(t, <-done)
	})
	connection, err := grpc.NewClient("passthrough:///registry", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	client := registrypb.NewRegistryServiceClient(connection)
	request := env.createSource("source", "source")
	request.NamespaceId, request.ExecutionId = env.id, BusinessID
	created, err := client.Mutate(testcontext.For(t), request)
	require.NoError(t, err)
	_, err = client.Mutate(testcontext.For(t), &registrypb.MutateRequest{
		NamespaceId: env.id, ExecutionId: BusinessID,
		Operation: &registrypb.MutateRequest_DeleteSource{DeleteSource: env.sourceOperation("source", 1, "delete")},
	})
	require.NoError(t, err)
	retry, err := client.Mutate(testcontext.For(t), request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, created, retry)
	response, err := client.Read(testcontext.For(t), &registrypb.ReadRequest{
		NamespaceId: env.id, ExecutionId: BusinessID, Operation: &registrypb.ReadRequest_Revision{
			Revision: &registrypb.RevisionLookup{
				Kind: registrypb.RESOURCE_KIND_SOURCE, Ref: &contractpb.ResourceRef{Id: "source", Revision: 1},
			},
		},
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &registrypb.Record{
		Resource: &registrypb.Record_Source{Source: &contractpb.Source{
			Namespace: env.namespace, Id: "source", Revision: 1, DesiredState: contractpb.DesiredState_DESIRED_STATE_STOPPED,
			Spec: &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}},
		}}, Desired: registrypb.DESIRED_STATE_STOPPED, Incarnation: "1",
	}, response.Records[0])
}
