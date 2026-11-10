//go:build temporal_with_drasi && cgo

package watch

import (
	"context"
	"errors"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/watch/native"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
)

func TestCompilerHostPublishesVerifiedExternalCapability(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		reviewed  bool
		reader    bool
		available bool
	}{
		{name: "default remains builtin only"},
		{name: "reader alone", reader: true},
		{name: "reviewed pair alone", reviewed: true},
		{name: "loaded reviewed pair and reader", reviewed: true, reader: true, available: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service, digest, id := pluginStatusService(t)
			namespaces := namespace.NewMockRegistry(gomock.NewController(t))
			namespaces.EXPECT().GetNamespaceByID(id).Return(
				namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: id.String(), Name: "test"}, nil, "active"), nil,
			)
			service.params.Namespaces = namespaces
			service.enabled = func(string) bool { return true }
			if test.reviewed {
				service.params.PluginPolicy = &PluginPolicy{ManagedPlugins: []native.ManagedPluginPair{{
					Source:    native.PluginComponentRef{Digest: digest, Component: "source"},
					Bootstrap: native.PluginComponentRef{Digest: digest, Component: "bootstrap"},
				}}}
			}
			if test.reader {
				service.params.Credentials = graphCredentialReader(func(context.Context, namespace.ID, native.SecretReference) ([]byte, error) {
					return nil, errors.New("capability must not resolve credentials")
				})
			}
			result, err := service.GetCapabilities(t.Context(), &watchservice.GetCapabilitiesRequest{NamespaceId: id.String()})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &contractpb.Capabilities{
				Enabled: true, ApiVersion: "v1", QueryLanguages: []string{"cypher", "gql"},
				StateStores: []string{"persistent"}, TemporalSource: true, Once: true,
				ExternalSources: test.available, MaxRequestBytes: validation.MaxRequestBytes,
			}, result)
		})
	}
}

func TestCompilerHostChecksNamespaceWithoutOpeningGraph(t *testing.T) {
	t.Parallel()
	id := testvars.New(t).NamespaceID().String()
	for _, tc := range []struct {
		name     string
		change   func(*watchservice.ValidateQueryRequest)
		disabled bool
		global   bool
		code     codes.Code
	}{
		{name: "valid"},
		{name: "invalid UUID", change: func(r *watchservice.ValidateQueryRequest) { r.NamespaceId = "test" }, code: codes.InvalidArgument},
		{name: "disabled", disabled: true, code: codes.FailedPrecondition},
		{name: "global", global: true, code: codes.FailedPrecondition},
		{name: "missing spec", change: func(r *watchservice.ValidateQueryRequest) { r.Spec = nil }, code: codes.InvalidArgument},
		{name: "unsupported language", change: func(r *watchservice.ValidateQueryRequest) { r.Spec.Language = "unknown" }, code: codes.InvalidArgument},
		{name: "unsupported store", change: func(r *watchservice.ValidateQueryRequest) { r.Spec.StateStore = "unknown" }, code: codes.InvalidArgument},
		{name: "oversized text", change: func(r *watchservice.ValidateQueryRequest) { r.Spec.Text = strings.Repeat("x", 65_537) }, code: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Native compiler concurrency is process-wide and tested independently.
			request := &watchservice.ValidateQueryRequest{
				NamespaceId: id,
				Spec: &contractpb.QuerySpec{
					Language: "cypher", Text: "MATCH (n) RETURN n", StateStore: "persistent",
					Sources: []*contractpb.ResourceRef{{Id: "source", Revision: 1}},
				},
			}
			if tc.change != nil {
				tc.change(request)
			}
			namespaces := namespace.NewMockRegistry(gomock.NewController(t))
			if request.NamespaceId == id {
				ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: id, Name: "test"}, nil, "active")
				if tc.global {
					ns = namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Id: id, Name: "test"}, nil, true, nil, 0)
				}
				namespaces.EXPECT().GetNamespaceByID(namespace.ID(id)).Return(ns, nil)
			}

			service := &Service{params: ServiceParams{Namespaces: namespaces}, enabled: func(string) bool { return !tc.disabled }}
			response, err := service.ValidateQuery(t.Context(), request)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			if tc.code == codes.OK {
				protorequire.ProtoEqual(t, &contractpb.ValidateQueryResponse{Valid: true}, response)
			} else {
				require.Nil(t, response)
			}
			require.Nil(t, service.namespaces)
		})
	}
}

func TestCompilerHostPreservesCallerCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	service := &Service{}
	result, err := service.ValidateQuery(ctx, &watchservice.ValidateQueryRequest{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	capabilities, err := service.GetCapabilities(ctx, &watchservice.GetCapabilitiesRequest{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, capabilities)
}

func TestCompilerHostChecksExternalSchemasWithoutOpeningGraphOrReadingProviders(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		reviewed bool
		text     string
		code     codes.Code
	}{
		{"unreviewed pair", false, "MATCH (n) RETURN n", codes.FailedPrecondition},
		{"reviewed pair without loaded factories", true, "MATCH (n) RETURN n", codes.FailedPrecondition},
		{"invalid syntax before schema work", false, "MATCH (", codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Native compiler concurrency is process-wide.
			request := sourceSchemaRequest()
			request.Spec.Text = tc.text
			var policy PluginPolicy
			if tc.reviewed {
				recipe := pluginSourceDefinition().Recipe.(native.PluginSourceRecipe)
				policy.ManagedPlugins = []native.ManagedPluginPair{{Source: recipe.Source.Plugin, Bootstrap: recipe.Bootstrap.Plugin}}
			}
			service := catalogRuntime(t, policy)
			service.enabled = func(string) bool { return true }
			namespaces := namespace.NewMockRegistry(gomock.NewController(t))
			namespaces.EXPECT().GetNamespaceByID(namespace.ID(request.NamespaceId)).Return(
				namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: request.NamespaceId, Name: "test"}, nil, "active"), nil,
			)
			service.params.Namespaces = namespaces
			reads := make(chan struct{}, 1)
			service.params.Credentials = graphCredentialReader(func(context.Context, namespace.ID, native.SecretReference) ([]byte, error) {
				select {
				case reads <- struct{}{}:
				default:
				}
				return nil, errors.New("schema validation must not resolve credentials")
			})
			response, err := service.ValidateBoundQuery(t.Context(), &watchservice.ValidateBoundQueryRequest{Validation: request})
			result := response.GetValidation()
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			if tc.code == codes.OK {
				protorequire.ProtoEqual(t, &contractpb.ValidateQueryResponse{Diagnostics: []*contractpb.Diagnostic{{
					Field: "spec.text", Message: "Query syntax or structure is invalid",
				}}}, result)
			} else {
				require.Nil(t, result)
			}
			require.Empty(t, reads)
			require.Nil(t, service.namespaces)
			observed, err := service.plugins.runtime.Apply(t.Context(), native.ObserveNodesCommand{})
			require.NoError(t, err)
			require.Empty(t, observed.Nodes)
		})
	}
}
