package watch

import (
	"context"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
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
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

type compilerTestServer struct {
	watchservice.UnimplementedWatchServiceServer
	response *contractpb.ValidateQueryResponse
	caps     *contractpb.Capabilities
	wait     bool
	requests chan *watchservice.ValidateQueryRequest
}

type compilerOnlyTestServer struct {
	watchservice.UnimplementedWatchServiceServer
	requests chan *watchservice.ValidateQueryRequest
}

func (s *compilerOnlyTestServer) ValidateQuery(_ context.Context, request *watchservice.ValidateQueryRequest) (*contractpb.ValidateQueryResponse, error) {
	s.requests <- common.CloneProto(request)
	return &contractpb.ValidateQueryResponse{Valid: true}, nil
}

func (s *compilerTestServer) ValidateQuery(ctx context.Context, request *watchservice.ValidateQueryRequest) (*contractpb.ValidateQueryResponse, error) {
	s.requests <- common.CloneProto(request)
	if s.wait {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return common.CloneProto(s.response), nil
}

func (s *compilerTestServer) GetCapabilities(context.Context, *watchservice.GetCapabilitiesRequest) (*contractpb.Capabilities, error) {
	return common.CloneProto(s.caps), nil
}

func (s *compilerTestServer) ValidateBoundQuery(ctx context.Context, request *watchservice.ValidateBoundQueryRequest) (*watchservice.ValidateBoundQueryResponse, error) {
	response, err := s.ValidateQuery(ctx, request.GetValidation())
	if err != nil {
		return nil, err
	}
	return &watchservice.ValidateBoundQueryResponse{Validation: response}, nil
}

func compilerObserver(t *testing.T, server watchservice.WatchServiceServer) (*Observer, func()) {
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
	resolver.EXPECT().Lookup("namespace-id").Return(membership.NewHostInfoFromAddress("compiler:7239"), nil)
	factory := common.NewMockRPCFactory(controller)
	factory.EXPECT().CreateWatchGRPCConnection("compiler:7239").Return(conn)
	return NewObserver(nil, monitor, factory), func() {
		rpcServer.Stop()
		require.NoError(t, await.Rcv(t, done))
	}
}

func TestCompilerRPCNeedsNoNamespaceAssignment(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		response    *contractpb.ValidateQueryResponse
		definitions []*contractpb.Source
		wait        bool
		code        codes.Code
	}{
		{name: "accepted", response: &contractpb.ValidateQueryResponse{Valid: true}},
		{name: "compiler diagnostics", response: &contractpb.ValidateQueryResponse{Diagnostics: []*contractpb.Diagnostic{{
			Field: "spec.text", Message: "Query syntax or structure is invalid",
		}}}},
		{name: "empty response", response: &contractpb.ValidateQueryResponse{}, code: codes.DataLoss},
		{name: "contradictory result", response: &contractpb.ValidateQueryResponse{Valid: true, Diagnostics: []*contractpb.Diagnostic{{}}}, code: codes.DataLoss},
		{name: "unredacted error", response: &contractpb.ValidateQueryResponse{Diagnostics: []*contractpb.Diagnostic{{
			Field: "spec.text", Message: "/private/path query contents",
		}}}, code: codes.DataLoss},
		{name: "source schema mismatch", definitions: []*contractpb.Source{{Spec: &contractpb.SourceSpec{
			Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{}},
		}}}, response: &contractpb.ValidateQueryResponse{Diagnostics: []*contractpb.Diagnostic{{
			Field: "spec.sources[0]", Message: "External source configuration does not match the pinned descriptors",
		}}}},
		{name: "accepted external source", definitions: []*contractpb.Source{{Spec: &contractpb.SourceSpec{
			Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{}},
		}}}, response: &contractpb.ValidateQueryResponse{Valid: true}},
		{name: "missing source-aware response", definitions: []*contractpb.Source{{Spec: &contractpb.SourceSpec{
			Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{}},
		}}}, code: codes.DataLoss},
		{name: "source-aware deadline", definitions: []*contractpb.Source{{Spec: &contractpb.SourceSpec{
			Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{}},
		}}}, wait: true, code: codes.DeadlineExceeded},
		{name: "source diagnostic without binding", response: &contractpb.ValidateQueryResponse{Diagnostics: []*contractpb.Diagnostic{{
			Field: "spec.sources[0]", Message: "External source configuration does not match the pinned descriptors",
		}}}, code: codes.DataLoss},
		{name: "bounded deadline", wait: true, code: codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := &compilerTestServer{response: tc.response, wait: tc.wait, requests: make(chan *watchservice.ValidateQueryRequest, 1)}
			observer, stop := compilerObserver(t, server)
			defer stop()
			spec := &contractpb.QuerySpec{Language: "cypher", Text: "MATCH (n) RETURN n"}
			response, err := observer.ValidateQuery(t.Context(), "namespace-id", spec, tc.definitions)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, &watchservice.ValidateQueryRequest{NamespaceId: "namespace-id", Spec: spec, SourceDefinitions: tc.definitions}, await.Rcv(t, server.requests))
			if tc.code == codes.OK {
				protorequire.ProtoEqual(t, tc.response, response)
			} else {
				require.Nil(t, response)
			}
		})
	}
}

func TestCompilerRPCOlderHostNeverApprovesExternalSources(t *testing.T) {
	t.Parallel()
	builtin := &contractpb.Source{Spec: &contractpb.SourceSpec{
		Kind: &contractpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}},
	}}
	external := &contractpb.Source{Spec: &contractpb.SourceSpec{
		Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{}},
	}}
	for _, tc := range []struct {
		name        string
		definitions []*contractpb.Source
		code        codes.Code
	}{
		{name: "legacy compiler"},
		{name: "builtin only", definitions: []*contractpb.Source{builtin}},
		{name: "external only", definitions: []*contractpb.Source{external}, code: codes.Unavailable},
		{name: "mixed bindings", definitions: []*contractpb.Source{builtin, external}, code: codes.Unavailable},
		{name: "unknown kind", definitions: []*contractpb.Source{{Spec: &contractpb.SourceSpec{}}}, code: codes.Unavailable},
		{name: "missing definition", definitions: []*contractpb.Source{nil}, code: codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := &compilerOnlyTestServer{requests: make(chan *watchservice.ValidateQueryRequest, 1)}
			observer, stop := compilerObserver(t, server)
			defer stop()
			response, err := observer.ValidateQuery(t.Context(), "namespace-id", &contractpb.QuerySpec{
				Language: "cypher", Text: "MATCH (n) RETURN n",
			}, tc.definitions)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			if tc.code != codes.OK {
				require.EqualError(t, err, "external source schema validation is unavailable on this Watch host")
				require.Nil(t, response)
				require.Empty(t, server.requests, "source-aware validation must never fall back to compilation")
			} else {
				require.NoError(t, err)
				protorequire.ProtoEqual(t, &contractpb.ValidateQueryResponse{Valid: true}, response)
				request := await.Rcv(t, server.requests)
				require.Len(t, request.SourceDefinitions, len(tc.definitions))
			}
		})
	}
}

func TestCompilerCapabilitiesRejectUnsupportedClaims(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		change   func(*contractpb.Capabilities)
		accepted bool
	}{
		{"actual support", func(*contractpb.Capabilities) {}, true},
		{"verified external sources", func(c *contractpb.Capabilities) { c.ExternalSources = true }, true},
		{"preview", func(c *contractpb.Capabilities) { c.MaxPreviewRows = 1 }, false},
		{"cross host recovery", func(c *contractpb.Capabilities) { c.CrossHostRecovery = true }, false},
		{"global namespaces", func(c *contractpb.Capabilities) { c.GlobalNamespaces = true }, false},
		{"host authorization", func(c *contractpb.Capabilities) { c.AllowedMethods = []string{"fake"} }, false},
		{"unknown language", func(c *contractpb.Capabilities) { c.QueryLanguages[0] = "unknown" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			caps := &contractpb.Capabilities{
				Enabled: true, ApiVersion: "v1", QueryLanguages: []string{"cypher", "gql"},
				StateStores: []string{"persistent"}, TemporalSource: true, Once: true, MaxRequestBytes: 1 << 20,
			}
			tc.change(caps)
			observer, stop := compilerObserver(t, &compilerTestServer{caps: caps})
			defer stop()
			result, err := observer.QueryCapabilities(t.Context(), "namespace-id")
			if tc.accepted {
				require.NoError(t, err)
				protorequire.ProtoEqual(t, caps, result)
			} else {
				require.Equal(t, codes.DataLoss, serviceerror.ToStatus(err).Code())
				require.Nil(t, result)
			}
		})
	}
}

func TestCompilerRPCRejectsOversizedBindingsBeforeMembershipIO(t *testing.T) {
	t.Parallel()
	observer := &Observer{}
	_, err := observer.ValidateQuery(t.Context(), "namespace-id", &contractpb.QuerySpec{}, []*contractpb.Source{{
		Spec: &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
			Configuration: &structpb.Struct{Fields: map[string]*structpb.Value{
				"literal": structpb.NewStringValue(strings.Repeat("x", 1<<20)),
			}},
		}}},
	}})
	require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
}

func TestCompilerRPCBoundsTheCompleteSourceAwareEnvelopeBeforeMembership(t *testing.T) {
	t.Parallel()
	value := structpb.NewStringValue(strings.Repeat("x", validation.MaxRequestBytes))
	request := &watchservice.ValidateQueryRequest{
		NamespaceId: "namespace-id", Spec: &contractpb.QuerySpec{},
		SourceDefinitions: []*contractpb.Source{{Spec: &contractpb.SourceSpec{
			Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
				Configuration: &structpb.Struct{Fields: map[string]*structpb.Value{"literal": value}},
			}},
		}}},
	}
	overhead := proto.Size(request) - len(value.GetStringValue())
	value.Kind = &structpb.Value_StringValue{StringValue: strings.Repeat("x", validation.MaxRequestBytes-overhead)}
	require.Equal(t, validation.MaxRequestBytes, proto.Size(request))
	require.Greater(t, proto.Size(&watchservice.ValidateBoundQueryRequest{Validation: request}), validation.MaxRequestBytes)
	observer := &Observer{}
	response, err := observer.ValidateQuery(t.Context(), request.NamespaceId, request.Spec, request.SourceDefinitions)
	require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
	require.Nil(t, response)
}

func TestCompilerRPCSchemaDiagnosticsAreBoundToExternalSourceIndices(t *testing.T) {
	t.Parallel()
	definitions := []*contractpb.Source{
		{Spec: &contractpb.SourceSpec{}},
		{Spec: &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{}}}},
	}
	for _, tc := range []struct {
		name     string
		field    string
		message  string
		accepted bool
	}{
		{"actual external source", "spec.sources[1]", "External source configuration does not match the pinned descriptors", true},
		{"builtin source", "spec.sources[0]", "External source configuration does not match the pinned descriptors", false},
		{"out of range", "spec.sources[2]", "External source configuration does not match the pinned descriptors", false},
		{"noncanonical index", "spec.sources[01]", "External source configuration does not match the pinned descriptors", false},
		{"private diagnostic", "spec.sources[1]", "private config or credential reference", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.accepted, queryDiagnostic([]*contractpb.Diagnostic{{
				Field: tc.field, Message: tc.message,
			}}, definitions))
		})
	}
}
