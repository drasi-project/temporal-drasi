package watch

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func sourceSchemaRequest() *watchservice.ValidateQueryRequest {
	component := func(pin, kind, version string) *contractpb.ComponentConfiguration {
		return &contractpb.ComponentConfiguration{
			Plugin: &contractpb.PluginComponentRef{Digest: "sha256:" + strings.Repeat(pin, 64), Component: kind},
			Secrets: map[string]*contractpb.SecretReference{
				"connection": {Provider: "provider", Key: "key", Version: version},
			},
		}
	}
	external := component("a", "source", "generation-1")
	external.Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{
		"nested": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"a": structpb.NewNumberValue(1), "b": structpb.NewNumberValue(2),
		}}),
	}}
	return &watchservice.ValidateQueryRequest{
		NamespaceId: "00000000-0000-4000-8000-000000000001",
		Spec: &contractpb.QuerySpec{
			Language: "cypher", Text: "MATCH (n) RETURN n", StateStore: "persistent",
			Sources: []*contractpb.ResourceRef{{Id: "builtin", Revision: 5}, {Id: "external", Revision: 9}},
		},
		SourceDefinitions: []*contractpb.Source{
			{Namespace: "test", Id: "builtin", Revision: 5, Spec: &contractpb.SourceSpec{
				Kind: &contractpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}},
			}},
			{Namespace: "test", Id: "external", Revision: 9, Spec: &contractpb.SourceSpec{
				Kind:      &contractpb.SourceSpec_External{External: external},
				Bootstrap: component("b", "bootstrap", "generation-2"),
			}},
		},
	}
}

func TestQuerySourceSchemasPreserveExactBindingsWithoutGraphOrProviders(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		change   func(*watchservice.ValidateQueryRequest)
		external bool
		code     codes.Code
	}{
		{name: "mixed sources", external: true},
		{name: "legacy compiler only", change: func(r *watchservice.ValidateQueryRequest) { r.SourceDefinitions = nil }},
		{name: "builtin only", change: func(r *watchservice.ValidateQueryRequest) {
			r.SourceDefinitions, r.Spec.Sources = r.SourceDefinitions[:1], r.Spec.Sources[:1]
		}},
		{name: "missing binding", change: func(r *watchservice.ValidateQueryRequest) { r.SourceDefinitions = r.SourceDefinitions[:1] }, code: codes.InvalidArgument},
		{name: "extra binding", change: func(r *watchservice.ValidateQueryRequest) {
			r.SourceDefinitions = append(r.SourceDefinitions, r.SourceDefinitions[0])
		}, code: codes.InvalidArgument},
		{name: "wrong namespace", change: func(r *watchservice.ValidateQueryRequest) { r.SourceDefinitions[1].Namespace = "another" }, code: codes.InvalidArgument},
		{name: "wrong source", change: func(r *watchservice.ValidateQueryRequest) { r.SourceDefinitions[1].Id = "another" }, code: codes.InvalidArgument},
		{name: "wrong revision", change: func(r *watchservice.ValidateQueryRequest) { r.SourceDefinitions[1].Revision++ }, code: codes.InvalidArgument},
		{name: "nil source", change: func(r *watchservice.ValidateQueryRequest) { r.SourceDefinitions[1] = nil }, code: codes.InvalidArgument},
		{name: "missing spec", change: func(r *watchservice.ValidateQueryRequest) { r.SourceDefinitions[1].Spec = nil }, code: codes.InvalidArgument},
		{name: "missing bootstrap", change: func(r *watchservice.ValidateQueryRequest) { r.SourceDefinitions[1].Spec.Bootstrap = nil }, code: codes.FailedPrecondition},
		{name: "missing credential version", change: func(r *watchservice.ValidateQueryRequest) {
			r.SourceDefinitions[1].Spec.GetExternal().Secrets["connection"].Version = ""
		}, code: codes.InvalidArgument},
		{name: "implicit credential reference", change: func(r *watchservice.ValidateQueryRequest) {
			r.SourceDefinitions[1].Spec.GetExternal().Configuration.Fields["nested"] = structpb.NewStringValue("${PRIVATE_REFERENCE}")
		}, code: codes.InvalidArgument},
		{name: "builtin bootstrap override", change: func(r *watchservice.ValidateQueryRequest) {
			r.SourceDefinitions[0].Spec.Bootstrap = r.SourceDefinitions[1].Spec.Bootstrap
		}, code: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := sourceSchemaRequest()
			if tc.change != nil {
				tc.change(request)
			}
			before := common.CloneProto(request)
			recipes, indices, err := querySourceRecipes(request, "test")
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, before, request)
			if !tc.external {
				require.Nil(t, recipes)
				require.Nil(t, indices)
				return
			}
			require.Equal(t, []int{1}, indices)
			require.Len(t, recipes, 1)
			expected, err := json.Marshal(pluginSourceDefinition().Recipe)
			require.NoError(t, err)
			actual, err := json.Marshal(recipes[0])
			require.NoError(t, err)
			require.JSONEq(t, string(expected), string(actual))
			request.SourceDefinitions[1].Spec.GetExternal().Secrets["connection"].Version = "later-version"
			require.Equal(t, "generation-1", recipes[0].Source.Secrets["connection"].Version)
		})
	}
}

func TestQuerySourceSchemaRPCRequiresDefinitions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		request *watchservice.ValidateBoundQueryRequest
	}{
		{name: "nil request"},
		{name: "missing validation", request: &watchservice.ValidateBoundQueryRequest{}},
		{name: "empty definitions", request: &watchservice.ValidateBoundQueryRequest{Validation: &watchservice.ValidateQueryRequest{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			service := &Service{}
			response, err := service.ValidateBoundQuery(t.Context(), tc.request)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			require.Nil(t, response)
		})
	}
}

func TestQuerySourceSchemaResultsRejectMissingAndUnknownVerdicts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		verdicts []native.SourceSchemaVerdict
		code     codes.Code
		want     *contractpb.ValidateQueryResponse
	}{
		{name: "valid", verdicts: []native.SourceSchemaVerdict{native.SourceSchemaValid, native.SourceSchemaValid}, want: &contractpb.ValidateQueryResponse{Valid: true}},
		{name: "invalid second source", verdicts: []native.SourceSchemaVerdict{native.SourceSchemaValid, native.SourceSchemaInvalidConfiguration}, want: &contractpb.ValidateQueryResponse{
			Diagnostics: []*contractpb.Diagnostic{{Field: "spec.sources[3]", Message: "External source configuration does not match the pinned descriptors"}},
		}},
		{name: "missing", code: codes.DataLoss},
		{name: "partial", verdicts: []native.SourceSchemaVerdict{native.SourceSchemaValid}, code: codes.DataLoss},
		{name: "extra", verdicts: []native.SourceSchemaVerdict{native.SourceSchemaValid, native.SourceSchemaValid, native.SourceSchemaValid}, code: codes.DataLoss},
		{name: "empty verdict", verdicts: []native.SourceSchemaVerdict{native.SourceSchemaValid, ""}, code: codes.DataLoss},
		{name: "unknown after rejection", verdicts: []native.SourceSchemaVerdict{native.SourceSchemaInvalidConfiguration, "unknown"}, code: codes.DataLoss},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result, err := querySchemaResponse(tc.verdicts, []int{0, 3})
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, tc.want, result)
		})
	}
}

func TestQuerySourceSchemaErrorsRemainDistinctAndRedacted(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"policy or unsupported schema", &native.Error{Code: native.CodeFailedPrecondition, Message: "private schema"}, codes.FailedPrecondition},
		{"capacity", &native.Error{Code: native.CodeConflict, Message: "private capacity"}, codes.ResourceExhausted},
		{"interrupted native work", &native.Error{Code: native.CodeCancelled, Message: "private work"}, codes.Unavailable},
		{"native deadline", &native.Error{Code: native.CodeDeadline, Message: "private deadline"}, codes.Unavailable},
		{"closed", native.ErrClosed, codes.Unavailable},
		{"disabled", native.ErrDisabled, codes.Unavailable},
		{"unavailable", serviceerror.NewUnavailable("private runtime"), codes.Unavailable},
		{"unexpected malformed input", &native.Error{Code: native.CodeInvalidArgument, Message: "private input"}, codes.Internal},
		{"unexpected missing resource", &native.Error{Code: native.CodeNotFound, Message: "private resource"}, codes.Internal},
		{"unexpected replay gap", &native.Error{Code: native.CodeGap, Message: "private replay"}, codes.Internal},
		{"unexpected native fault", &native.Error{Code: native.CodeInternal, Message: "private internal failure"}, codes.Internal},
		{"unknown native code", &native.Error{Code: 99, Message: "private unknown code"}, codes.DataLoss},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			service := &Service{params: ServiceParams{Logger: log.NewNoopLogger()}}
			err := service.querySchemaError(t.Context(), tc.err)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			require.NotContains(t, err.Error(), "private")
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, service.querySchemaError(ctx, tc.err), context.Canceled)
		})
	}
}
