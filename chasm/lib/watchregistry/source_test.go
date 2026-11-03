package watchregistry

import (
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

func externalSourceSpec() *contractpb.SourceSpec {
	return &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
		Plugin: &contractpb.PluginComponentRef{Digest: "sha256:" + strings.Repeat("a", 64), Component: "fixture-source"},
		Secrets: map[string]*contractpb.SecretReference{
			"connection": {Provider: "credentials", Key: "reader", Version: "generation-17"},
		},
	}}}
}

func TestRegistryFreshSourceCredentialRejectionsAreDurable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*contractpb.SourceSpec)
		field  string
	}{
		{"unversioned source", func(spec *contractpb.SourceSpec) {
			spec.GetExternal().Secrets["connection"].Version = ""
		}, "secrets.version"},
		{"unversioned bootstrap", func(spec *contractpb.SourceSpec) {
			spec.Bootstrap = common.CloneProto(spec.GetExternal())
			spec.Bootstrap.Secrets["connection"].Version = ""
		}, "spec.bootstrap.secrets.version"},
		{"ambient environment", func(spec *contractpb.SourceSpec) {
			spec.GetExternal().Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{
				"host": structpb.NewStringValue("${HOST:-localhost}"),
			}}
		}, "configuration"},
		{"inline secret overlap", func(spec *contractpb.SourceSpec) {
			spec.GetExternal().Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{
				"connection": structpb.NewStringValue("inline-value"),
			}}
		}, "configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t, DefaultLimits())
			request := env.createSource("source", "rejected")
			request.GetCreateSource().Spec = externalSourceSpec()
			tc.change(request.GetCreateSource().Spec)
			result, err := env.mutate(request)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			require.ErrorContains(t, err, tc.field)
			require.Nil(t, result)
			before := env.state(t)
			require.Zero(t, before.Generation)
			require.Empty(t, before.Current)
			require.Empty(t, before.Revisions)
			require.Len(t, before.Receipts, 1)
			_, retryError := env.mutate(request)
			require.EqualError(t, retryError, err.Error())
			protorequire.ProtoEqual(t, before, env.state(t))
			changed := common.CloneProto(request)
			changed.GetCreateSource().Spec = externalSourceSpec()
			_, err = env.mutate(changed)
			require.ErrorContains(t, err, "different method or content")
			changed.GetCreateSource().RequestId = "accepted"
			result, err = env.mutate(changed)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, externalSourceSpec(), result.Record.GetSource().Spec)
		})
	}
}

func TestRegistrySourceCredentialRotationPreservesPinnedRevision(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	request := env.createSource("source", "create")
	request.GetCreateSource().Spec = externalSourceSpec()
	request.GetCreateSource().Spec.Bootstrap = common.CloneProto(request.GetCreateSource().Spec.GetExternal())
	created, err := env.mutate(request)
	require.NoError(t, err)
	_, err = env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	spec := common.CloneProto(request.GetCreateSource().Spec)
	spec.GetExternal().Secrets["connection"].Version = "generation-18"
	spec.Bootstrap.Secrets["connection"].Version = "bootstrap-19"
	updated, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateSource{
		UpdateSource: &contractpb.UpdateSourceRequest{
			Namespace: env.namespace, Id: "source", ExpectedRevision: 1, RequestId: "rotate", Spec: spec,
		},
	}})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &registrypb.Record{
		Resource: &registrypb.Record_Source{Source: &contractpb.Source{
			Namespace: env.namespace, Id: "source", Revision: 2, Spec: spec,
			DesiredState: contractpb.DesiredState_DESIRED_STATE_STOPPED,
		}},
		Incarnation: "2", Desired: registrypb.DESIRED_STATE_STOPPED,
	}, updated.Record)
	pinned, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_QuerySources{
		QuerySources: &contractpb.ValidateQueryRequest{Namespace: env.namespace, Spec: querySpec("source", 1)},
	}})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &registrypb.ReadResponse{
		Generation: 3, Records: []*registrypb.Record{created.Record},
	}, pinned)
	retried, err := env.mutate(request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, created, retried)
}

func TestRegistrySourceCredentialReplacementDoesNotAdvanceOnRejection(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	request := env.createSource("source", "create")
	request.GetCreateSource().Spec = externalSourceSpec()
	created, err := env.mutate(request)
	require.NoError(t, err)
	spec := common.CloneProto(request.GetCreateSource().Spec)
	spec.GetExternal().Secrets["connection"].Version = ""
	_, err = env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateSource{
		UpdateSource: &contractpb.UpdateSourceRequest{
			Namespace: env.namespace, Id: "source", ExpectedRevision: 1, RequestId: "rejected", Spec: spec,
		},
	}})
	require.ErrorContains(t, err, "secrets.version")
	state := env.state(t)
	require.Equal(t, uint64(1), state.Generation)
	protorequire.ProtoEqual(t, created.Record, state.Current[resourceKey(registrypb.RESOURCE_KIND_SOURCE, "source")])
	require.Len(t, state.Revisions, 1)
	require.Len(t, state.Receipts, 2)
}

func TestRegistryLegacySourceCredentialsKeepReceiptAndReadCompatibility(t *testing.T) {
	t.Parallel()
	request := &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_CreateSource{
		CreateSource: &contractpb.CreateSourceRequest{
			Namespace: "test", Id: "source", RequestId: "legacy", Spec: externalSourceSpec(),
		},
	}}
	request.GetCreateSource().Spec.GetExternal().Secrets["connection"].Version = ""
	request.GetCreateSource().Spec.GetExternal().Configuration = &structpb.Struct{Fields: map[string]*structpb.Value{
		"host": structpb.NewStringValue("${HOST}"),
	}}
	require.NoError(t, validation.ValidateRequest(request.GetCreateSource()))
	info, err := decodeMutation(request)
	require.NoError(t, err)
	record, err := definition(info, nil, "test", 1)
	require.NoError(t, err)
	fingerprint, err := mutationFingerprint(request)
	require.NoError(t, err)
	result := &registrypb.MutationResult{Record: record}
	component, err := newComponent("test", DefaultLimits())
	require.NoError(t, err)
	key := resourceKey(registrypb.RESOURCE_KIND_SOURCE, "source")
	component.Generation = 1
	component.Current[key] = common.CloneProto(record)
	component.Revisions[revisionKey(key, 1)] = common.CloneProto(record)
	component.Receipts["legacy"] = &registrypb.Receipt{
		Fingerprint: fingerprint[:], Result: common.CloneProto(result),
	}
	before := common.CloneProto(component.State)
	retried, err := component.apply(nil, mutationInput{
		request: request, info: info, acceptanceError: "Drasi watches are disabled for the namespace",
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, result, retried)
	retried.Record.GetSource().Spec.GetExternal().Secrets["connection"].Version = "modified-response"
	protorequire.ProtoEqual(t, before, component.State)
	fresh := &registrypb.MutateRequest{Operation: &registrypb.MutateRequest_UpdateSource{
		UpdateSource: &contractpb.UpdateSourceRequest{
			Namespace: "test", Id: "source", ExpectedRevision: 1, RequestId: "fresh",
			Spec: common.CloneProto(request.GetCreateSource().Spec),
		},
	}}
	freshInfo, err := decodeMutation(fresh)
	require.NoError(t, err)
	rejected, err := component.apply(nil, mutationInput{request: fresh, info: freshInfo})
	require.NoError(t, err)
	require.Equal(t, registrypb.REJECTION_CODE_INVALID_ARGUMENT, rejected.Rejection.Code)
	require.Nil(t, rejected.Record)
	require.Equal(t, before.Generation, component.Generation)
	protorequire.ProtoEqual(t, before.Current[key], component.Current[key])
	protorequire.ProtoEqual(t, before.Revisions[revisionKey(key, 1)], component.Revisions[revisionKey(key, 1)])
	protorequire.ProtoEqual(t, before.Receipts["legacy"], component.Receipts["legacy"])
	require.Len(t, component.Receipts, 2)
	before = common.CloneProto(component.State)
	for _, tc := range []struct {
		name    string
		request *registrypb.ReadRequest
	}{
		{"get", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_GetSource{
			GetSource: &contractpb.GetSourceRequest{Namespace: "test", Id: "source"},
		}}},
		{"list", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListSources{
			ListSources: &contractpb.ListSourcesRequest{Namespace: "test"},
		}}},
		{"revision", &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_Revision{
			Revision: &registrypb.RevisionLookup{
				Kind: registrypb.RESOURCE_KIND_SOURCE, Ref: &contractpb.ResourceRef{Id: "source", Revision: 1},
			},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			read, err := decodeRead(tc.request, "test")
			require.NoError(t, err)
			response, err := component.read(nil, read)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &registrypb.ReadResponse{
				Generation: 1, Records: []*registrypb.Record{record},
			}, response)
			response.Records[0].GetSource().Spec.GetExternal().Secrets["connection"].Version = "modified-read"
			protorequire.ProtoEqual(t, before, component.State)
		})
	}
}
