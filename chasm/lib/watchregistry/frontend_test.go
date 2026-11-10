package watchregistry

import (
	"context"
	"errors"
	"net"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	rpcinterceptor "go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type frontendRegistryClient struct {
	registrypb.RegistryServiceClient
	mutate func(context.Context, *registrypb.MutateRequest) (*registrypb.MutationResult, error)
	read   func(context.Context, *registrypb.ReadRequest) (*registrypb.ReadResponse, error)
}

type frontendNamespaceChecker struct {
	namespaces namespace.Registry
}

func (c frontendNamespaceChecker) Exists(name namespace.Name) error {
	_, err := c.namespaces.GetNamespace(name)
	return err
}

func (c frontendRegistryClient) Mutate(ctx context.Context, request *registrypb.MutateRequest, _ ...grpc.CallOption) (*registrypb.MutationResult, error) {
	return c.mutate(ctx, request)
}

func (c frontendRegistryClient) Read(ctx context.Context, request *registrypb.ReadRequest, _ ...grpc.CallOption) (*registrypb.ReadResponse, error) {
	return c.read(ctx, request)
}

func frontendForTest(t *testing.T, client registrypb.RegistryServiceClient) *frontendHandler {
	t.Helper()
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
		Id: "00000000-0000-4000-8000-000000000001", Name: "test",
	}, nil, "active")
	namespaces := namespace.NewMockRegistry(gomock.NewController(t))
	namespaces.EXPECT().GetNamespaceID(ns.Name()).Return(ns.ID(), nil).AnyTimes()
	namespaces.EXPECT().GetNamespace(ns.Name()).Return(ns, nil).AnyTimes()
	return &frontendHandler{
		client: client, namespaces: namespaces, enabled: func(string) bool { return true },
		authorizer: authorization.NewDefaultAuthorizer(), logger: log.NewNoopLogger(),
	}
}

func TestFrontendRejectsMalformedMutationResponses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		result *registrypb.MutationResult
	}{
		{"missing response", nil},
		{"missing resource", &registrypb.MutationResult{}},
		{"wrong resource kind", &registrypb.MutationResult{Record: &registrypb.Record{
			Desired: registrypb.DESIRED_STATE_RUNNING,
			Resource: &registrypb.Record_Query{Query: &contractpb.Query{
				Namespace: "test", Id: "query", Revision: 1,
			}},
		}}},
		{"nil source", &registrypb.MutationResult{Record: &registrypb.Record{
			Desired: registrypb.DESIRED_STATE_RUNNING, Resource: &registrypb.Record_Source{},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := frontendForTest(t, frontendRegistryClient{mutate: func(context.Context, *registrypb.MutateRequest) (*registrypb.MutationResult, error) {
				return tc.result, nil
			}})
			result, err := h.CreateSource(t.Context(), &contractpb.CreateSourceRequest{
				Namespace: "test", Id: "source", RequestId: "create", Spec: sourceSpec(),
			})
			require.Error(t, err)
			require.Nil(t, result)
		})
	}
}

func TestPublicRecordProjectsDesiredStateWithoutObservations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		private registrypb.DesiredState
		public  contractpb.DesiredState
	}{
		{"stopped", registrypb.DESIRED_STATE_STOPPED, contractpb.DesiredState_DESIRED_STATE_STOPPED},
		{"running", registrypb.DESIRED_STATE_RUNNING, contractpb.DesiredState_DESIRED_STATE_RUNNING},
		{"paused", registrypb.DESIRED_STATE_PAUSED, contractpb.DesiredState_DESIRED_STATE_PAUSED},
		{"deleted", registrypb.DESIRED_STATE_DELETED, contractpb.DesiredState_DESIRED_STATE_DELETED},
		{"canceled", registrypb.DESIRED_STATE_CANCELED, contractpb.DesiredState_DESIRED_STATE_CANCELED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, kind := range []string{"source", "query", "watch"} {
				t.Run(kind, func(t *testing.T) {
					t.Parallel()
					input := &registrypb.Record{Desired: tc.private}
					expected := &registrypb.Record{Desired: tc.private}
					switch kind {
					case "source":
						input.Resource = &registrypb.Record_Source{Source: &contractpb.Source{Id: "source"}}
						expected.Resource = &registrypb.Record_Source{Source: &contractpb.Source{Id: "source", DesiredState: tc.public}}
					case "query":
						input.Resource = &registrypb.Record_Query{Query: &contractpb.Query{Id: "query"}}
						expected.Resource = &registrypb.Record_Query{Query: &contractpb.Query{Id: "query", DesiredState: tc.public}}
					case "watch":
						input.Resource = &registrypb.Record_Watch{Watch: &contractpb.Watch{Id: "watch"}}
						expected.Resource = &registrypb.Record_Watch{Watch: &contractpb.Watch{Id: "watch", DesiredState: tc.public}}
					default:
						t.Fatalf("unexpected resource kind %q", kind)
					}
					before := common.CloneProto(input)
					result, err := publicRecord(input)
					require.NoError(t, err)
					protorequire.ProtoEqual(t, expected, result)
					protorequire.ProtoEqual(t, before, input)
					require.NotSame(t, input, result)
				})
			}

		})
	}
}

func TestFrontendMutationRouting(t *testing.T) {
	t.Parallel()
	createSource := &contractpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "create-source", Spec: sourceSpec()}
	updateSource := &contractpb.UpdateSourceRequest{Namespace: "test", Id: "source", RequestId: "update-source", ExpectedRevision: 7, Spec: sourceSpec()}
	sourceOp := &contractpb.SourceOperationRequest{Namespace: "test", Id: "source", RequestId: "source-operation", ExpectedRevision: 7}
	createQuery := &contractpb.CreateQueryRequest{Namespace: "test", Id: "query", RequestId: "create-query", Spec: querySpec("source", 7)}
	updateQuery := &contractpb.UpdateQueryRequest{Namespace: "test", Id: "query", RequestId: "update-query", ExpectedRevision: 7, Spec: querySpec("source", 7)}
	queryOp := &contractpb.QueryOperationRequest{Namespace: "test", Id: "query", RequestId: "query-operation", ExpectedRevision: 7}
	createWatch := &contractpb.CreateWatchRequest{Namespace: "test", Id: "watch", RequestId: "create-watch", Spec: watchSpec("query", 7)}
	updateWatch := &contractpb.UpdateWatchRequest{Namespace: "test", Id: "watch", RequestId: "update-watch", ExpectedRevision: 7, Spec: watchSpec("query", 7)}
	watchOp := &contractpb.WatchOperationRequest{Namespace: "test", Id: "watch", RequestId: "watch-operation", ExpectedRevision: 7}
	resetWatch := &contractpb.ResetWatchRequest{Namespace: "test", Id: "watch", RequestId: "reset-watch", ExpectedRevision: 7, InitialResults: contractpb.InitialResults_INITIAL_RESULTS_INCLUDE}
	source := &registrypb.Record{Desired: registrypb.DESIRED_STATE_RUNNING, Resource: &registrypb.Record_Source{Source: &contractpb.Source{
		Namespace: "test", Id: "source", Revision: 8, Spec: sourceSpec(), DesiredState: contractpb.DesiredState_DESIRED_STATE_RUNNING,
	}}}
	query := &registrypb.Record{Desired: registrypb.DESIRED_STATE_RUNNING, Resource: &registrypb.Record_Query{Query: &contractpb.Query{
		Namespace: "test", Id: "query", Revision: 8, Spec: querySpec("source", 7), DesiredState: contractpb.DesiredState_DESIRED_STATE_RUNNING,
	}}}
	watch := &registrypb.Record{Desired: registrypb.DESIRED_STATE_RUNNING, Resource: &registrypb.Record_Watch{Watch: &contractpb.Watch{
		Namespace: "test", Id: "watch", Revision: 8, Spec: watchSpec("query", 7), DesiredState: contractpb.DesiredState_DESIRED_STATE_RUNNING,
	}}}
	for _, tc := range []struct {
		name    string
		request *registrypb.MutateRequest
		record  *registrypb.Record
		call    func(context.Context, *frontendHandler) (proto.Message, error)
	}{
		{"create source", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateSource{CreateSource: createSource}}, source,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.CreateSource(ctx, createSource)
			}},
		{"update source", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateSource{UpdateSource: updateSource}}, source,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.UpdateSource(ctx, updateSource)
			}},
		{"delete source", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{DeleteSource: sourceOp}}, source,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.DeleteSource(ctx, sourceOp)
			}},
		{"start source", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartSource{StartSource: sourceOp}}, source,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.StartSource(ctx, sourceOp)
			}},
		{"stop source", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StopSource{StopSource: sourceOp}}, source,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.StopSource(ctx, sourceOp)
			}},
		{"create query", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateQuery{CreateQuery: createQuery}}, query,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.CreateQuery(ctx, createQuery)
			}},
		{"update query", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateQuery{UpdateQuery: updateQuery}}, query,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.UpdateQuery(ctx, updateQuery)
			}},
		{"delete query", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteQuery{DeleteQuery: queryOp}}, query,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.DeleteQuery(ctx, queryOp)
			}},
		{"start query", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StartQuery{StartQuery: queryOp}}, query,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.StartQuery(ctx, queryOp)
			}},
		{"stop query", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_StopQuery{StopQuery: queryOp}}, query,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) { return h.StopQuery(ctx, queryOp) }},
		{"create watch", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateWatch{CreateWatch: createWatch}}, watch,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.CreateWatch(ctx, createWatch)
			}},
		{"update watch", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateWatch{UpdateWatch: updateWatch}}, watch,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.UpdateWatch(ctx, updateWatch)
			}},
		{"delete watch", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteWatch{DeleteWatch: watchOp}}, watch,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.DeleteWatch(ctx, watchOp)
			}},
		{"pause watch", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_PauseWatch{PauseWatch: watchOp}}, watch,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.PauseWatch(ctx, watchOp)
			}},
		{"resume watch", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_ResumeWatch{ResumeWatch: watchOp}}, watch,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.ResumeWatch(ctx, watchOp)
			}},
		{"cancel watch", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CancelWatch{CancelWatch: watchOp}}, watch,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.CancelWatch(ctx, watchOp)
			}},
		{"reset watch", &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_ResetWatch{ResetWatch: resetWatch}}, watch,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.ResetWatch(ctx, resetWatch)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expected := common.CloneProto(tc.request)
			expected.NamespaceId, expected.ExecutionId = "00000000-0000-4000-8000-000000000001", BusinessID
			calls := 0
			h := frontendForTest(t, frontendRegistryClient{mutate: func(_ context.Context, request *registrypb.MutateRequest) (*registrypb.MutationResult, error) {
				calls++
				protorequire.ProtoEqual(t, expected, request)
				return &registrypb.MutationResult{Record: tc.record}, nil
			}})
			result, err := tc.call(t.Context(), h)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, resource(tc.record), result)
			require.Equal(t, 1, calls)
		})
	}
}

func TestFrontendReadRouting(t *testing.T) {
	t.Parallel()
	source := &contractpb.Source{Namespace: "test", Id: "source", Revision: 7, DesiredState: contractpb.DesiredState_DESIRED_STATE_STOPPED}
	query := &contractpb.Query{Namespace: "test", Id: "query", Revision: 7, DesiredState: contractpb.DesiredState_DESIRED_STATE_STOPPED}
	watch := &contractpb.Watch{Namespace: "test", Id: "watch", Revision: 7, DesiredState: contractpb.DesiredState_DESIRED_STATE_RUNNING}
	getSource := &contractpb.GetSourceRequest{Namespace: "test", Id: source.Id}
	getQuery := &contractpb.GetQueryRequest{Namespace: "test", Id: query.Id}
	getWatch := &contractpb.GetWatchRequest{Namespace: "test", Id: watch.Id}
	listSources := &contractpb.ListSourcesRequest{Namespace: "test", PageSize: 7, PageToken: "page"}
	listQueries := &contractpb.ListQueriesRequest{Namespace: "test", PageSize: 7, PageToken: "page"}
	listWatches := &contractpb.ListWatchesRequest{Namespace: "test", PageSize: 7, PageToken: "page",
		Owner: &commonpb.WorkflowExecution{WorkflowId: "owner", RunId: "00000000-0000-4000-8000-000000000002"}}
	for _, tc := range []struct {
		name     string
		request  *registrypb.ReadRequest
		record   *registrypb.Record
		expected proto.Message
		call     func(context.Context, *frontendHandler) (proto.Message, error)
	}{
		{"get source", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_GetSource{GetSource: getSource}},
			&registrypb.Record{Resource: &registrypb.Record_Source{Source: source}, Desired: registrypb.DESIRED_STATE_STOPPED}, source,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.GetSource(ctx, getSource)
			}},
		{"get query", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_GetQuery{GetQuery: getQuery}},
			&registrypb.Record{Resource: &registrypb.Record_Query{Query: query}, Desired: registrypb.DESIRED_STATE_STOPPED}, query,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) { return h.GetQuery(ctx, getQuery) }},
		{"get watch", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_GetWatch{GetWatch: getWatch}},
			&registrypb.Record{Resource: &registrypb.Record_Watch{Watch: watch}, Desired: registrypb.DESIRED_STATE_RUNNING}, watch,
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) { return h.GetWatch(ctx, getWatch) }},
		{"list sources", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListSources{ListSources: listSources}},
			&registrypb.Record{Resource: &registrypb.Record_Source{Source: source}, Desired: registrypb.DESIRED_STATE_STOPPED},
			&contractpb.ListSourcesResponse{Sources: []*contractpb.Source{source}, NextPageToken: "next"},
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.ListSources(ctx, listSources)
			}},
		{"list queries", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListQueries{ListQueries: listQueries}},
			&registrypb.Record{Resource: &registrypb.Record_Query{Query: query}, Desired: registrypb.DESIRED_STATE_STOPPED},
			&contractpb.ListQueriesResponse{Queries: []*contractpb.Query{query}, NextPageToken: "next"},
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.ListQueries(ctx, listQueries)
			}},
		{"list watches", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListWatches{ListWatches: listWatches}},
			&registrypb.Record{Resource: &registrypb.Record_Watch{Watch: watch}, Desired: registrypb.DESIRED_STATE_RUNNING},
			&contractpb.ListWatchesResponse{Watches: []*contractpb.Watch{watch}, NextPageToken: "next"},
			func(ctx context.Context, h *frontendHandler) (proto.Message, error) {
				return h.ListWatches(ctx, listWatches)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expected := common.CloneProto(tc.request)
			expected.NamespaceId, expected.ExecutionId = "00000000-0000-4000-8000-000000000001", BusinessID
			private := &registrypb.ReadResponse{Records: []*registrypb.Record{tc.record}, Generation: 7, NextPageToken: "next"}
			before := common.CloneProto(private)
			h := frontendForTest(t, frontendRegistryClient{read: func(_ context.Context, request *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
				protorequire.ProtoEqual(t, expected, request)
				return private, nil
			}})
			result, err := tc.call(t.Context(), h)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.expected, result)
			protorequire.ProtoEqual(t, before, private)
		})
	}
}

func TestFrontendCapabilities(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		role    authorization.Role
		enabled bool
	}{
		{"reader", authorization.RoleReader, true},
		{"writer", authorization.RoleWriter, true},
		{"disabled writer", authorization.RoleWriter, false},
		{"no claims", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := frontendForTest(t, nil)
			h.enabled = func(string) bool { return tc.enabled }
			ctx := context.WithValue(t.Context(), authorization.MappedClaims, &authorization.Claims{Namespaces: map[string]authorization.Role{"test": tc.role}})
			result, err := h.GetCapabilities(ctx, &contractpb.GetCapabilitiesRequest{Namespace: "test"})
			require.NoError(t, err)
			expected := &contractpb.Capabilities{
				Enabled: tc.enabled, ApiVersion: "v1", MaxRequestBytes: DefaultLimits().RequestBytes,
				Message: "Durable definitions are supported; native runtime capabilities are not yet available",
			}
			if !tc.enabled {
				expected.Message = "Drasi watches are disabled for the namespace"
			}
			switch tc.role {
			case authorization.RoleReader:
				expected.AllowedMethods = []string{
					contractpb.WatchService_GetCapabilities_FullMethodName,
					contractpb.WatchService_GetSource_FullMethodName, contractpb.WatchService_ListSources_FullMethodName,
					contractpb.WatchService_GetQuery_FullMethodName, contractpb.WatchService_ListQueries_FullMethodName,
					contractpb.WatchService_GetWatch_FullMethodName, contractpb.WatchService_ListWatches_FullMethodName,
					contractpb.WatchService_ListDeliveries_FullMethodName,
				}
			case authorization.RoleWriter:
				expected.AllowedMethods = definitionMethods
				require.Len(t, result.AllowedMethods, 26)
			case 0:
			default:
				t.Fatalf("unexpected authorization role %v", tc.role)
			}
			protorequire.ProtoEqual(t, expected, result)
			for _, unsupported := range []string{
				contractpb.WatchService_ValidateQuery_FullMethodName, contractpb.WatchService_PreviewQuery_FullMethodName,
			} {
				require.NotContains(t, result.AllowedMethods, unsupported)
			}
		})
	}
}

func TestFrontendCapabilityAuthorizationError(t *testing.T) {
	t.Parallel()
	h := frontendForTest(t, nil)
	authorizer := authorization.NewMockAuthorizer(gomock.NewController(t))
	authorizer.EXPECT().Authorize(gomock.Any(), gomock.Any(), gomock.Any()).Return(authorization.Result{}, errors.New("unavailable"))
	h.authorizer = authorizer
	result, err := h.GetCapabilities(t.Context(), &contractpb.GetCapabilitiesRequest{Namespace: "test"})
	require.ErrorContains(t, err, "Drasi capability authorization failed")
	require.Equal(t, codes.Internal, serviceerror.ToStatus(err).Code())
	require.Nil(t, result)
}

func TestFrontendGlobalNamespaceCapabilities(t *testing.T) {
	t.Parallel()
	h := frontendForTest(t, nil)
	namespaces := namespace.NewMockRegistry(gomock.NewController(t))
	namespaces.EXPECT().GetNamespace(namespace.Name("test")).Return(
		namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "test"}, nil, true, nil, 0), nil)
	h.namespaces = namespaces
	result, err := h.GetCapabilities(t.Context(), &contractpb.GetCapabilitiesRequest{Namespace: "test"})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &contractpb.Capabilities{
		ApiVersion: "v1", MaxRequestBytes: DefaultLimits().RequestBytes,
		Message: "Drasi watches do not support global namespaces",
	}, result)
}

func TestFrontendReadErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		response *registrypb.ReadResponse
		err      error
	}{
		{"missing response", nil, nil},
		{"missing resource", &registrypb.ReadResponse{Records: []*registrypb.Record{nil}}, nil},
		{"nil source", &registrypb.ReadResponse{Records: []*registrypb.Record{{
			Desired: registrypb.DESIRED_STATE_STOPPED, Resource: &registrypb.Record_Source{},
		}}}, nil},
		{"wrong resource kind", &registrypb.ReadResponse{Records: []*registrypb.Record{{
			Desired: registrypb.DESIRED_STATE_STOPPED, Resource: &registrypb.Record_Query{Query: &contractpb.Query{Id: "query"}},
		}}}, nil},
		{"unknown desired state", &registrypb.ReadResponse{Records: []*registrypb.Record{{
			Desired: registrypb.DesiredState(100), Resource: &registrypb.Record_Source{Source: &contractpb.Source{Id: "source"}},
		}}}, nil},
		{"History error", nil, serviceerror.NewUnavailable("History unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, method := range []string{"get", "list"} {
				t.Run(method, func(t *testing.T) {
					t.Parallel()
					h := frontendForTest(t, frontendRegistryClient{read: func(context.Context, *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
						return tc.response, tc.err
					}})
					var result proto.Message
					var err error
					switch method {
					case "get":
						result, err = h.GetSource(t.Context(), &contractpb.GetSourceRequest{Namespace: "test", Id: "source"})
					case "list":
						result, err = h.ListSources(t.Context(), &contractpb.ListSourcesRequest{Namespace: "test"})
					default:
						t.Fatalf("unexpected read method %q", method)
					}
					require.Nil(t, result)
					if tc.err != nil {
						require.ErrorIs(t, err, tc.err)
					} else {
						require.Equal(t, codes.Internal, serviceerror.ToStatus(err).Code())
					}
				})
			}
		})
	}
}

func TestFrontendGRPCAuthorization(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		claims *authorization.Claims
		code   codes.Code
	}{
		{"anonymous", nil, codes.PermissionDenied},
		{"reader", &authorization.Claims{Namespaces: map[string]authorization.Role{"test": authorization.RoleReader}}, codes.PermissionDenied},
		{"other namespace", &authorization.Claims{Namespaces: map[string]authorization.Role{"other": authorization.RoleWriter}}, codes.PermissionDenied},
		{"writer", &authorization.Claims{Namespaces: map[string]authorization.Role{"test": authorization.RoleWriter}}, codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requests := make(chan *registrypb.MutateRequest, 1)
			h := frontendForTest(t, frontendRegistryClient{mutate: func(_ context.Context, request *registrypb.MutateRequest) (*registrypb.MutationResult, error) {
				requests <- common.CloneProto(request)
				return &registrypb.MutationResult{Record: &registrypb.Record{
					Desired: registrypb.DESIRED_STATE_STOPPED, Resource: &registrypb.Record_Source{Source: &contractpb.Source{
						Namespace: "test", Id: "source", Revision: 1, Spec: sourceSpec(),
					}},
				}}, nil
			}})
			mapper := authorization.NewMockClaimMapper(gomock.NewController(t))
			mapper.EXPECT().GetClaims(gomock.Any()).Return(tc.claims, nil)
			interceptor := authorization.NewInterceptor(
				mapper, h.authorizer, metrics.NoopMetricsHandler, log.NewNoopLogger(), frontendNamespaceChecker{h.namespaces}, nil, "", "",
				dynamicconfig.GetBoolPropertyFn(false), dynamicconfig.GetBoolPropertyFn(false),
				dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false), dynamicconfig.GetBoolPropertyFn(false),
			)
			errorInterceptor := rpcinterceptor.NewServiceErrorInterceptor(dynamicconfig.GetIntPropertyFn(4<<20), metrics.NoopMetricsHandler, log.NewNoopLogger())
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(errorInterceptor.Intercept, interceptor.Intercept))
			contractpb.RegisterWatchServiceServer(server, validation.WrapWatchService(h))
			listener := bufconn.Listen(1 << 20)
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, listener.Close())
				require.NoError(t, <-done)
			})
			conn, err := grpc.NewClient("passthrough:///drasi-frontend",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "test-token")
			request := &contractpb.CreateSourceRequest{Namespace: "test", Id: "source", RequestId: "create", Spec: sourceSpec()}
			result, err := contractpb.NewWatchServiceClient(conn).CreateSource(ctx, request)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			if tc.code == codes.OK {
				require.NoError(t, err)
				protorequire.ProtoEqual(t, &contractpb.Source{
					Namespace: "test", Id: "source", Revision: 1, Spec: sourceSpec(), DesiredState: contractpb.DesiredState_DESIRED_STATE_STOPPED,
				}, result)
				protorequire.ProtoEqual(t, &registrypb.MutateRequest{
					NamespaceId: "00000000-0000-4000-8000-000000000001", ExecutionId: BusinessID,
					Operation: &registrypb.MutateRequest_CreateSource{CreateSource: request},
				}, await.Rcv(t, requests))
			} else {
				require.Nil(t, result)
				select {
				case request := <-requests:
					t.Fatalf("unauthorized mutation reached History: %v", request)
				default:
				}
			}
		})
	}
}
