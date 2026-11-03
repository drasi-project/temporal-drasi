package watchregistry

import (
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestRegistryConsistentPagination(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	request := &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListSources{
		ListSources: &contractpb.ListSourcesRequest{Namespace: env.namespace, PageSize: 2},
	}}
	empty, err := env.read(request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &registrypb.ReadResponse{}, empty)
	for _, id := range []string{"c", "a", "b"} {
		_, err := env.mutate(env.createSource(id, id))
		require.NoError(t, err)
	}
	first, err := env.read(request)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, sourceIDs(first))
	require.NotEmpty(t, first.NextPageToken)
	request.GetListSources().PageToken = first.NextPageToken
	second, err := env.read(request)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, sourceIDs(second))
	require.Empty(t, second.NextPageToken)
	require.Equal(t, first.Generation, second.Generation)
	_, err = env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListQueries{
		ListQueries: &contractpb.ListQueriesRequest{Namespace: env.namespace, PageToken: first.NextPageToken},
	}})
	require.ErrorContains(t, err, "page token does not match")
	_, err = env.mutate(env.createSource("d", "d"))
	require.NoError(t, err)
	_, err = env.read(request)
	require.ErrorContains(t, err, "restart the list")
}

func sourceIDs(response *registrypb.ReadResponse) []string {
	ids := make([]string, 0, len(response.Records))
	for _, record := range response.Records {
		ids = append(ids, record.GetSource().Id)
	}
	return ids
}

func TestQuerySourceLookupIsAtomicAndReadOnly(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	for _, id := range []string{"first", "second"} {
		_, err := env.mutate(env.createSource(id, id))
		require.NoError(t, err)
	}
	_, err := env.mutate(env.createQuery("first", "second", 1))
	require.NoError(t, err)
	before := env.state(t)
	spec := querySpec("second", 1)
	spec.Sources = append(spec.Sources, &contractpb.ResourceRef{Id: "first", Revision: 1})
	request := &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_QuerySources{
		QuerySources: &contractpb.ValidateQueryRequest{Namespace: env.namespace, Spec: spec},
	}}
	response, err := env.read(request)
	require.NoError(t, err)
	require.Equal(t, before.Generation, response.Generation)
	require.Equal(t, []string{"second", "first"}, sourceIDs(response))
	response.Records[0].GetSource().Id = "changed"
	protorequire.ProtoEqual(t, before, env.state(t))
	spec.Sources[1].Revision = 999
	response, err = env.read(request)
	require.ErrorContains(t, err, "pinned dependency revision does not exist")
	require.Nil(t, response)
	protorequire.ProtoEqual(t, before, env.state(t))
}

func TestQuerySourceLookupRejectsMissingDeletedAndCrossKindPins(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		source  bool
		deleted bool
		query   bool
	}{
		{name: "missing"},
		{name: "deleted", source: true, deleted: true},
		{name: "query with same ID", query: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newEnvironment(t, DefaultLimits())
			_, err := env.mutate(env.createSource("dependency", "dependency"))
			require.NoError(t, err)
			if tc.source {
				_, err := env.mutate(env.createSource("source", "source"))
				require.NoError(t, err)
			}
			if tc.deleted {
				_, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteSource{
					DeleteSource: env.sourceOperation("source", 1, "delete"),
				}})
				require.NoError(t, err)
			}
			if tc.query {
				_, err := env.mutate(env.createQuery("source", "dependency", 1))
				require.NoError(t, err)
			}
			before := env.state(t)
			response, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_QuerySources{
				QuerySources: &contractpb.ValidateQueryRequest{Namespace: env.namespace, Spec: querySpec("source", 1)},
			}})
			require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
			require.Nil(t, response)
			protorequire.ProtoEqual(t, before, env.state(t))
		})
	}
}

func TestQuerySourceLookupHonorsExactPersistedByteLimit(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	for _, id := range []string{"first", "second"} {
		_, err := env.mutate(env.createSource(id, id))
		require.NoError(t, err)
	}
	spec := querySpec("first", 1)
	spec.Sources = append(spec.Sources, &contractpb.ResourceRef{Id: "second", Revision: 1})
	request := &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_QuerySources{
		QuerySources: &contractpb.ValidateQueryRequest{Namespace: env.namespace, Spec: spec},
	}}
	expected, err := env.read(request)
	require.NoError(t, err)
	before := env.state(t)
	for _, tc := range []struct {
		name   string
		excess uint32
	}{
		{"at byte limit", 0},
		{"one byte over", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			component := &Component{State: common.CloneProto(before)}
			component.Limits.PageBytes = uint32(proto.Size(expected)) - tc.excess
			input, err := decodeRead(request, env.namespace)
			require.NoError(t, err)
			response, err := component.read(nil, input)
			if tc.excess == 0 {
				require.NoError(t, err)
				protorequire.ProtoEqual(t, expected, response)
			} else {
				require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
				require.Nil(t, response)
			}
		})
	}
}

func TestRegistryByteBoundedPagesNeverSkipResources(t *testing.T) {
	t.Parallel()
	limits := DefaultLimits()
	limits.RequestBytes, limits.PageBytes = 8192, 12288
	env := newEnvironment(t, limits)
	for _, id := range []string{"a", "b", "c"} {
		request := env.createSource(id, id)
		request.GetCreateSource().Spec = &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
			Plugin: &contractpb.PluginComponentRef{Digest: "sha256:" + strings.Repeat("a", 64), Component: "postgres"},
			Configuration: &structpb.Struct{Fields: map[string]*structpb.Value{
				"configuration": structpb.NewStringValue(strings.Repeat("x", 7000)),
			}},
		}}}
		_, err := env.mutate(request)
		require.NoError(t, err)
	}
	request := &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListSources{
		ListSources: &contractpb.ListSourcesRequest{Namespace: env.namespace, PageSize: 200},
	}}
	var ids []string
	for range 3 {
		response, err := env.read(request)
		require.NoError(t, err)
		require.Len(t, response.Records, 1)
		require.LessOrEqual(t, proto.Size(response), int(limits.PageBytes))
		enriched := common.CloneProto(response)
		for _, record := range enriched.Records {
			record.GetSource().Status = &contractpb.ResourceStatus{Message: strings.Repeat("x", watchclient.MaxStatusBytes-3)}
			require.Equal(t, watchclient.MaxStatusBytes, proto.Size(record.GetSource().Status))
		}
		require.LessOrEqual(t, proto.Size(enriched), int(limits.PageBytes))
		ids = append(ids, sourceIDs(response)...)
		request.GetListSources().PageToken = response.NextPageToken
	}
	require.Empty(t, request.GetListSources().PageToken)
	require.Equal(t, []string{"a", "b", "c"}, ids)
}

func TestRegistryListsIncludeDesiredCleanupAndOwnerFilter(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "source"))
	require.NoError(t, err)
	_, err = env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	_, err = env.mutate(env.createWatch("watch", watchSpec("query", 1)))
	require.NoError(t, err)
	deleted, err := env.mutate(&registrypb.MutateRequest{Operation: &registrypb.MutateRequest_DeleteWatch{
		DeleteWatch: env.watchOperation("watch", 1, "delete"),
	}})
	require.NoError(t, err)
	listed, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListWatches{
		ListWatches: &contractpb.ListWatchesRequest{Namespace: env.namespace},
	}})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, deleted.Record, listed.Records[0])
	filtered, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListWatches{
		ListWatches: &contractpb.ListWatchesRequest{
			Namespace: env.namespace, Owner: &commonpb.WorkflowExecution{WorkflowId: "owner", RunId: "00000000-0000-4000-8000-000000000001"},
		},
	}})
	require.NoError(t, err)
	require.Empty(t, filtered.Records)
}

func TestRegistryResultsAreDetachedFromPersistentState(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	request := env.createSource("source", "source")
	accepted, err := env.mutate(request)
	require.NoError(t, err)
	expected := common.CloneProto(accepted)
	accepted.Record.GetSource().Id = "mutated-response"
	request.GetCreateSource().Spec.Kind = nil
	readRequest := &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_GetSource{
		GetSource: &contractpb.GetSourceRequest{Namespace: env.namespace, Id: "source"},
	}}
	read, err := env.read(readRequest)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected.Record, read.Records[0])
	read.Records[0].GetSource().Spec.Kind = nil
	read, err = env.read(readRequest)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected.Record, read.Records[0])
}

func TestRegistryStatusReservationBoundsTheEnrichedPublicResponse(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		request := env.createSource(id, id)
		request.GetCreateSource().Spec = &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
			Plugin: &contractpb.PluginComponentRef{Digest: "sha256:" + strings.Repeat("a", 64), Component: "postgres"},
			Configuration: &structpb.Struct{Fields: map[string]*structpb.Value{
				"configuration": structpb.NewStringValue(strings.Repeat("x", 128)),
			}},
		}}}
		_, err := env.mutate(request)
		require.NoError(t, err)
	}
	request := &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_ListSources{
		ListSources: &contractpb.ListSourcesRequest{Namespace: env.namespace, PageSize: 8},
	}}
	original, err := env.read(request)
	require.NoError(t, err)
	component := &Component{State: env.state(t)}
	component.Limits.PageBytes = uint32(proto.Size(original) + 1024)
	var ids []string
	for {
		input, err := decodeRead(request, env.namespace)
		require.NoError(t, err)
		page, err := component.list(input)
		require.NoError(t, err)
		require.NotEmpty(t, page.Records)
		public := &contractpb.ListSourcesResponse{NextPageToken: page.NextPageToken}
		for _, record := range page.Records {
			source := record.GetSource()
			source.Status = &contractpb.ResourceStatus{Message: strings.Repeat("x", watchclient.MaxStatusBytes-3)}
			public.Sources = append(public.Sources, source)
			ids = append(ids, source.Id)
		}
		require.LessOrEqual(t, proto.Size(public), int(component.Limits.PageBytes))
		if page.NextPageToken == "" {
			break
		}
		request.GetListSources().PageToken = page.NextPageToken
	}
	require.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g", "h"}, ids)
}
