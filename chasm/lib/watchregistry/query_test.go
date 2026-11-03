package watchregistry

import (
	"context"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
)

func TestQueryValidationChecksAuthoritativeSourceBindings(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*registrypb.ReadResponse)
		code   codes.Code
		want   *contractpb.ValidateQueryResponse
	}{
		{name: "no compiler", code: codes.Unavailable},
		{name: "missing snapshot", change: func(r *registrypb.ReadResponse) { r.Generation = 0 }, code: codes.DataLoss},
		{name: "missing source", change: func(r *registrypb.ReadResponse) { r.Records = nil }, code: codes.DataLoss},
		{name: "wrong namespace", change: func(r *registrypb.ReadResponse) { r.Records[0].GetSource().Namespace = "another" }, code: codes.DataLoss},
		{name: "wrong revision", change: func(r *registrypb.ReadResponse) { r.Records[0].GetSource().Revision++ }, code: codes.DataLoss},
		{name: "missing lifetime", change: func(r *registrypb.ReadResponse) { r.Records[0].Incarnation = "" }, code: codes.DataLoss},
		{name: "wrong kind", change: func(r *registrypb.ReadResponse) {
			r.Records[0].Resource = &registrypb.Record_Query{Query: &contractpb.Query{Id: "source"}}
		}, code: codes.DataLoss},
		{name: "invalid schema", change: func(r *registrypb.ReadResponse) {
			r.Records[0].GetSource().Spec = &contractpb.SourceSpec{}
		}, code: codes.DataLoss},
		{name: "unsupported schema", change: func(r *registrypb.ReadResponse) {
			r.Records[0].GetSource().Spec = &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
				Plugin: &contractpb.PluginComponentRef{Digest: "sha256:" + strings.Repeat("a", 64), Component: "postgres"},
			}}}
		}, code: codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			snapshot := &registrypb.ReadResponse{Generation: 7, Records: []*registrypb.Record{{
				Incarnation: "1", Resource: &registrypb.Record_Source{Source: &contractpb.Source{
					Namespace: "test", Id: "source", Revision: 1, Spec: sourceSpec(),
				}},
			}}}
			if tc.change != nil {
				tc.change(snapshot)
			}
			before := common.CloneProto(snapshot)
			request := &contractpb.ValidateQueryRequest{Namespace: "test", Spec: querySpec("source", 1)}
			handler := frontendForTest(t, frontendRegistryClient{read: func(_ context.Context, r *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
				protorequire.ProtoEqual(t, request, r.GetQuerySources())
				require.Equal(t, "00000000-0000-4000-8000-000000000001", r.NamespaceId)
				require.Equal(t, BusinessID, r.ExecutionId)
				return snapshot, nil
			}})
			response, err := handler.ValidateQuery(t.Context(), request)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, tc.want, response)
			protorequire.ProtoEqual(t, before, snapshot)
		})
	}
}

func TestQueryValidationRejectsUnsupportedSpecsBeforeRuntimeWork(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*contractpb.QuerySpec)
		want   *contractpb.ValidateQueryResponse
	}{
		{"language", func(s *contractpb.QuerySpec) { s.Language = "unknown" }, invalidQuery("spec.language", "Query language is not supported")},
		{"store", func(s *contractpb.QuerySpec) { s.StateStore = "unknown" }, invalidQuery("spec.state_store", "Query state store is not supported")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler := frontendForTest(t, nil)
			request := &contractpb.ValidateQueryRequest{Namespace: "test", Spec: querySpec("source", 1)}
			tc.change(request.Spec)
			response, err := handler.ValidateQuery(t.Context(), request)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, tc.want, response)
			handler.enabled = func(string) bool { return false }
			response, err = handler.ValidateQuery(t.Context(), request)
			require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
			require.Nil(t, response)
		})
	}
}

func TestCapabilitiesWithDisabledAuthorizationMatchFrontendPolicy(t *testing.T) {
	t.Parallel()
	handler := frontendForTest(t, nil)
	handler.authorizer = nil
	result, err := handler.GetCapabilities(t.Context(), &contractpb.GetCapabilitiesRequest{Namespace: "test"})
	require.NoError(t, err)
	require.Equal(t, definitionMethods, result.AllowedMethods)
}

func TestQueryCompilationErrorsPreserveStatusAndRedactDetails(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"concurrency", serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "private"),
			serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT, "native compiler concurrency limit reached")},
		{"source binding bytes", serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "private"),
			serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "query validation source bindings exceed the request byte limit")},
		{"deadline", serviceerror.NewDeadlineExceeded("private"), serviceerror.NewDeadlineExceeded("native query compilation deadline exceeded")},
		{"cancelled", serviceerror.NewCanceled("private"), serviceerror.NewCanceled("native query compilation canceled")},
		{"invalid response", serviceerror.NewDataLoss("private"), serviceerror.NewDataLoss("native query compilation returned an invalid response")},
		{"internal", serviceerror.NewInternal("private"), serviceerror.NewInternal("native query compilation failed")},
		{"schema unavailable", serviceerror.NewFailedPrecondition("private"), serviceerror.NewFailedPrecondition("external source schema validation is unavailable")},
		{"unavailable", serviceerror.NewUnavailable("private"), serviceerror.NewUnavailable("native query compilation is unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, queryCompilationError(t.Context(), tc.err))
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, queryCompilationError(ctx, tc.err), context.Canceled)
		})
	}
}
