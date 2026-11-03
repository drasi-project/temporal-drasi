package watch

import (
	"context"
	"fmt"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/drasi-project/temporal-drasi/contracts/watch/validation"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchregistry"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

type desiredReadStep struct {
	request  *registrypb.ReadRequest
	response *registrypb.ReadResponse
	err      error
}

type desiredTestClient struct {
	registrypb.RegistryServiceClient
	t     *testing.T
	steps []desiredReadStep
	calls int
}

func (c *desiredTestClient) Read(ctx context.Context, request *registrypb.ReadRequest, _ ...grpc.CallOption) (*registrypb.ReadResponse, error) {
	c.t.Helper()
	require.NoError(c.t, ctx.Err())
	require.Less(c.t, c.calls, len(c.steps), "unexpected registry read")
	step := c.steps[c.calls]
	c.calls++
	expected := common.CloneProto(step.request)
	expected.NamespaceId, expected.ExecutionId = request.NamespaceId, watchregistry.BusinessID
	require.Equal(c.t, "00000000-0000-4000-8000-000000000001", request.NamespaceId)
	protorequire.ProtoEqual(c.t, expected, request)
	return step.response, step.err
}

func desiredList(kind registrypb.ResourceKind, token string) *registrypb.ReadRequest {
	request := &registrypb.ReadRequest{}
	switch kind {
	case registrypb.RESOURCE_KIND_SOURCE:
		request.Operation = &registrypb.ReadRequest_ListSources{ListSources: &contractpb.ListSourcesRequest{
			Namespace: "ns", PageSize: validation.MaxPageSize, PageToken: token,
		}}
	case registrypb.RESOURCE_KIND_QUERY:
		request.Operation = &registrypb.ReadRequest_ListQueries{ListQueries: &contractpb.ListQueriesRequest{
			Namespace: "ns", PageSize: validation.MaxPageSize, PageToken: token,
		}}
	case registrypb.RESOURCE_KIND_WATCH:
		request.Operation = &registrypb.ReadRequest_ListWatches{ListWatches: &contractpb.ListWatchesRequest{
			Namespace: "ns", PageSize: validation.MaxPageSize, PageToken: token,
		}}
	default:
		panic("unsupported desired view test resource kind")
	}
	return request
}

func desiredRevision(kind registrypb.ResourceKind, ref *contractpb.ResourceRef) *registrypb.ReadRequest {
	return &registrypb.ReadRequest{Operation: &registrypb.ReadRequest_Revision{
		Revision: &registrypb.RevisionLookup{Kind: kind, Ref: ref},
	}}
}

func desiredRecords() (source, query, watch *registrypb.Record) {
	source = &registrypb.Record{
		Resource: &registrypb.Record_Source{Source: &contractpb.Source{
			Namespace: "ns", Id: "source", Revision: 1,
			Spec: &contractpb.SourceSpec{Kind: &contractpb.SourceSpec_Temporal{Temporal: &emptypb.Empty{}}},
		}},
		Desired: registrypb.DESIRED_STATE_STOPPED, Incarnation: "1",
	}
	query = &registrypb.Record{
		Resource: &registrypb.Record_Query{Query: &contractpb.Query{
			Namespace: "ns", Id: "query", Revision: 1,
			Spec: &contractpb.QuerySpec{
				Language: "cypher", Text: "MATCH (n) RETURN n", StateStore: "persistent",
				Sources: []*contractpb.ResourceRef{{Id: "source", Revision: 1}},
			},
		}},
		Desired: registrypb.DESIRED_STATE_STOPPED, Incarnation: "1",
	}
	watch = &registrypb.Record{
		Resource: &registrypb.Record_Watch{Watch: &contractpb.Watch{
			Namespace: "ns", Id: "watch", Revision: 1,
			Spec: &contractpb.WatchSpec{Query: &contractpb.ResourceRef{Id: "query", Revision: 1}},
		}},
		Desired: registrypb.DESIRED_STATE_PAUSED, Incarnation: "1",
	}
	return source, query, watch
}

func desiredSteps(source, query, watch *registrypb.Record) []desiredReadStep {
	return []desiredReadStep{
		{request: desiredList(registrypb.RESOURCE_KIND_SOURCE, ""),
			response: &registrypb.ReadResponse{Generation: 7, Records: []*registrypb.Record{source}}},
		{request: desiredList(registrypb.RESOURCE_KIND_QUERY, ""),
			response: &registrypb.ReadResponse{Generation: 7, Records: []*registrypb.Record{query}}},
		{request: desiredList(registrypb.RESOURCE_KIND_WATCH, ""),
			response: &registrypb.ReadResponse{Generation: 7, Records: []*registrypb.Record{watch}}},
	}
}

func TestDesiredViewPreservesRetainedPinsAndCurrentLifecycle(t *testing.T) {
	t.Parallel()
	source, query, watch := desiredRecords()
	source3 := common.CloneProto(source)
	source3.GetSource().Revision, source3.Incarnation = 3, "3"
	currentSource := common.CloneProto(source3)
	currentSource.GetSource().Revision = 4
	currentQuery := common.CloneProto(query)
	currentQuery.GetQuery().Revision, currentQuery.Incarnation = 3, "3"
	currentQuery.GetQuery().Spec.Sources[0].Revision = 3
	currentQuery.Desired = registrypb.DESIRED_STATE_RUNNING
	steps := desiredSteps(currentSource, currentQuery, watch)
	steps = append(steps,
		desiredReadStep{request: desiredRevision(registrypb.RESOURCE_KIND_QUERY, watch.GetWatch().Spec.Query),
			response: &registrypb.ReadResponse{Generation: 7, Records: []*registrypb.Record{query}}},
		desiredReadStep{request: desiredRevision(registrypb.RESOURCE_KIND_SOURCE, query.GetQuery().Spec.Sources[0]),
			response: &registrypb.ReadResponse{Generation: 7, Records: []*registrypb.Record{source}}},
		desiredReadStep{request: desiredRevision(registrypb.RESOURCE_KIND_SOURCE, currentQuery.GetQuery().Spec.Sources[0]),
			response: &registrypb.ReadResponse{Generation: 7, Records: []*registrypb.Record{source3}}},
	)
	client := &desiredTestClient{t: t, steps: steps}
	view, err := ReadDesiredView(testcontext.For(t), client, "00000000-0000-4000-8000-000000000001", "ns")
	require.NoError(t, err)
	require.Equal(t, len(steps), client.calls)
	sourceKey := resourceKey{registrypb.RESOURCE_KIND_SOURCE, "source"}
	queryKey := resourceKey{registrypb.RESOURCE_KIND_QUERY, "query"}
	watchKey := resourceKey{registrypb.RESOURCE_KIND_WATCH, "watch"}
	expected := &DesiredView{
		namespaceID: "00000000-0000-4000-8000-000000000001", namespace: "ns", generation: 7,
		current: map[resourceKey]*registrypb.Record{sourceKey: currentSource, queryKey: currentQuery, watchKey: watch},
		revisions: map[definitionPin]*registrypb.Record{
			{sourceKey, 1}: source, {sourceKey, 3}: source3, {sourceKey, 4}: currentSource,
			{queryKey, 1}: query, {queryKey, 3}: currentQuery, {watchKey, 1}: watch,
		},
	}
	require.Empty(t, cmp.Diff(expected, view, cmp.AllowUnexported(DesiredView{}, resourceKey{}, definitionPin{}), protocmp.Transform()))
	pinned, err := view.Source(&contractpb.ResourceRef{Id: "source", Revision: 1})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, source, pinned)
	pinned.GetSource().Spec.Kind = nil
	source.GetSource().Spec.Kind = nil
	pinned, err = view.Source(&contractpb.ResourceRef{Id: "source", Revision: 1})
	require.NoError(t, err)
	require.NotNil(t, pinned.GetSource().Spec.Kind)
	pinnedQuery, err := view.Query(watch.GetWatch().Spec.Query)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, query, pinnedQuery)
	pinnedWatch, err := view.Watch("watch")
	require.NoError(t, err)
	protorequire.ProtoEqual(t, watch, pinnedWatch)
	require.Equal(t, uint64(7), view.Generation())
	_, err = view.Query(&contractpb.ResourceRef{Id: "query", Revision: 2})
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	_, err = view.Source(nil)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	_, err = view.Watch("missing")
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
}

func TestDesiredViewPreservesMissingLegacyIncarnations(t *testing.T) {
	t.Parallel()
	source, query, watch := desiredRecords()
	source.Incarnation, query.Incarnation = "", ""
	client := &desiredTestClient{t: t, steps: desiredSteps(source, query, watch)}
	view, err := ReadDesiredView(testcontext.For(t), client, "00000000-0000-4000-8000-000000000001", "ns")
	require.NoError(t, err)
	pinnedSource, err := view.Source(query.GetQuery().Spec.Sources[0])
	require.NoError(t, err)
	protorequire.ProtoEqual(t, source, pinnedSource)
	pinnedQuery, err := view.Query(watch.GetWatch().Spec.Query)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, query, pinnedQuery)
	require.Equal(t, len(client.steps), client.calls)
}

func TestDesiredViewRejectsInvalidScopeBeforeIO(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		namespaceID string
		namespace   string
	}{
		{"missing UUID", "", "ns"},
		{"nil UUID", "00000000-0000-0000-0000-000000000000", "ns"},
		{"malformed UUID", "not-a-uuid", "ns"},
		{"noncanonical UUID", "00000000000040008000000000000001", "ns"},
		{"uppercase UUID", "00000000-0000-4000-8000-00000000000A", "ns"},
		{"missing namespace", "00000000-0000-4000-8000-000000000001", ""},
		{"invalid namespace", "00000000-0000-4000-8000-000000000001", "../ns"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &desiredTestClient{t: t}
			view, err := ReadDesiredView(testcontext.For(t), client, tc.namespaceID, tc.namespace)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			require.Nil(t, view)
			require.Zero(t, client.calls)
		})
	}
}

func TestDesiredViewRejectsMixedOrMalformedReads(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func([]desiredReadStep) []desiredReadStep
		code   codes.Code
	}{
		{"mutation across resource lists", func(steps []desiredReadStep) []desiredReadStep {
			steps[1].response.Generation++
			return steps[:2]
		}, codes.FailedPrecondition},
		{"mutation within pagination", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.NextPageToken = "next"
			return []desiredReadStep{steps[0], {
				request:  desiredList(registrypb.RESOURCE_KIND_SOURCE, "next"),
				response: &registrypb.ReadResponse{Generation: 8},
			}}
		}, codes.FailedPrecondition},
		{"creation during empty read", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response = &registrypb.ReadResponse{}
			return steps[:2]
		}, codes.FailedPrecondition},
		{"nil response", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response = nil
			return steps[:1]
		}, codes.DataLoss},
		{"zero generation with records", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Generation = 0
			return steps[:1]
		}, codes.DataLoss},
		{"wrong resource kind", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0] = steps[1].response.Records[0]
			return steps[:1]
		}, codes.DataLoss},
		{"nil record", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0] = nil
			return steps[:1]
		}, codes.DataLoss},
		{"nil oneof payload", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0].Resource = &registrypb.Record_Source{}
			return steps[:1]
		}, codes.DataLoss},
		{"wrong namespace", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0].GetSource().Namespace = "another"
			return steps[:1]
		}, codes.DataLoss},
		{"invalid ID", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0].GetSource().Id = "../source"
			return steps[:1]
		}, codes.DataLoss},
		{"zero revision", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0].GetSource().Revision = 0
			return steps[:1]
		}, codes.DataLoss},
		{"missing spec", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0].GetSource().Spec = nil
			return steps[:1]
		}, codes.DataLoss},
		{"nil source kind payload", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0].GetSource().Spec.Kind = &contractpb.SourceSpec_Temporal{}
			return steps[:1]
		}, codes.DataLoss},
		{"unknown desired state", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0].Desired = registrypb.DesiredState(999)
			return steps[:1]
		}, codes.DataLoss},
		{"empty page with continuation", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records, steps[0].response.NextPageToken = nil, "next"
			return steps[:1]
		}, codes.DataLoss},
		{"duplicate record", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records = append(steps[0].response.Records, steps[0].response.Records[0])
			return steps[:1]
		}, codes.DataLoss},
		{"repeated page", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.NextPageToken = "next"
			return []desiredReadStep{steps[0], {
				request: desiredList(registrypb.RESOURCE_KIND_SOURCE, "next"), response: steps[0].response,
			}}
		}, codes.DataLoss},
		{"missing current dependency", func(steps []desiredReadStep) []desiredReadStep {
			steps[1].response.Records = nil
			return steps
		}, codes.DataLoss},
		{"incomplete dependency", func(steps []desiredReadStep) []desiredReadStep {
			steps[2].response.Records[0].GetWatch().Spec.Query.Revision = 0
			return steps
		}, codes.DataLoss},
		{"future dependency", func(steps []desiredReadStep) []desiredReadStep {
			steps[2].response.Records[0].GetWatch().Spec.Query.Revision = 2
			return steps
		}, codes.DataLoss},
		{"oversized page", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].response.Records[0].GetSource().Spec.Kind = &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
				Configuration: &structpb.Struct{Fields: map[string]*structpb.Value{
					"configuration": structpb.NewStringValue(strings.Repeat("x", watchregistry.MaxPageBytes)),
				}},
			}}
			return steps[:1]
		}, codes.DataLoss},
		{"RPC failure", func(steps []desiredReadStep) []desiredReadStep {
			steps[0].err = serviceerror.NewUnavailable("registry unavailable")
			return steps[:1]
		}, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source, query, watch := desiredRecords()
			steps := tc.change(desiredSteps(source, query, watch))
			client := &desiredTestClient{t: t, steps: steps}
			view, err := ReadDesiredView(testcontext.For(t), client, "00000000-0000-4000-8000-000000000001", "ns")
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			require.Nil(t, view)
			require.Equal(t, len(steps), client.calls)
		})
	}
}

func TestDesiredViewFencesRetainedLookups(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*registrypb.ReadResponse)
		code   codes.Code
	}{
		{"mutation during retained lookup", func(response *registrypb.ReadResponse) { response.Generation++ }, codes.FailedPrecondition},
		{"missing retained record", func(response *registrypb.ReadResponse) { response.Records = nil }, codes.DataLoss},
		{"extra retained record", func(response *registrypb.ReadResponse) {
			response.Records = append(response.Records, response.Records[0])
		}, codes.DataLoss},
		{"wrong revision", func(response *registrypb.ReadResponse) { response.Records[0].GetQuery().Revision++ }, codes.DataLoss},
		{"wrong ID", func(response *registrypb.ReadResponse) { response.Records[0].GetQuery().Id = "another" }, codes.DataLoss},
		{"unexpected continuation", func(response *registrypb.ReadResponse) { response.NextPageToken = "next" }, codes.DataLoss},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source, query, watch := desiredRecords()
			current := common.CloneProto(query)
			current.GetQuery().Revision = 2
			response := &registrypb.ReadResponse{Generation: 7, Records: []*registrypb.Record{query}}
			tc.change(response)
			steps := append(desiredSteps(source, current, watch), desiredReadStep{
				request:  desiredRevision(registrypb.RESOURCE_KIND_QUERY, watch.GetWatch().Spec.Query),
				response: response,
			})
			client := &desiredTestClient{t: t, steps: steps}
			view, err := ReadDesiredView(testcontext.For(t), client, "00000000-0000-4000-8000-000000000001", "ns")
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			require.Nil(t, view)
			require.Equal(t, len(steps), client.calls)
		})
	}
}

func TestDesiredViewBoundsAndEmptyRegistry(t *testing.T) {
	t.Parallel()
	for _, count := range []int{0, watchregistry.MaxResourceIDs, watchregistry.MaxResourceIDs + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			t.Parallel()
			var steps []desiredReadStep
			for start := 0; start < count; start += validation.MaxPageSize {
				token, next := "", ""
				if start != 0 {
					token = fmt.Sprint(start)
				}
				if start+validation.MaxPageSize < count {
					next = fmt.Sprint(start + validation.MaxPageSize)
				}
				response := &registrypb.ReadResponse{Generation: uint64(count), NextPageToken: next}
				for i := start; i < min(start+validation.MaxPageSize, count); i++ {
					source, _, _ := desiredRecords()
					source.GetSource().Id = fmt.Sprintf("source-%03d", i)
					response.Records = append(response.Records, source)
				}
				steps = append(steps, desiredReadStep{request: desiredList(registrypb.RESOURCE_KIND_SOURCE, token), response: response})
			}
			if count == 0 {
				steps = append(steps, desiredReadStep{
					request: desiredList(registrypb.RESOURCE_KIND_SOURCE, ""), response: &registrypb.ReadResponse{},
				})
			}
			if count <= watchregistry.MaxResourceIDs {
				for _, kind := range []registrypb.ResourceKind{registrypb.RESOURCE_KIND_QUERY, registrypb.RESOURCE_KIND_WATCH} {
					steps = append(steps, desiredReadStep{request: desiredList(kind, ""), response: &registrypb.ReadResponse{Generation: uint64(count)}})
				}
			}
			client := &desiredTestClient{t: t, steps: steps}
			view, err := ReadDesiredView(testcontext.For(t), client, "00000000-0000-4000-8000-000000000001", "ns")
			if count > watchregistry.MaxResourceIDs {
				require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
				require.Nil(t, view)
			} else {
				require.NoError(t, err)
				require.Len(t, view.current, count)
				require.Len(t, view.revisions, count)
			}
			require.Equal(t, len(steps), client.calls)
		})
	}
}

func TestDesiredViewRetainedByteLimit(t *testing.T) {
	t.Parallel()
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			t.Parallel()
			const count = 32
			var steps []desiredReadStep
			for start := 0; start < count; start += 7 {
				token, next := "", ""
				if start != 0 {
					token = fmt.Sprint(start)
				}
				if start+7 < count {
					next = fmt.Sprint(start + 7)
				}
				response := &registrypb.ReadResponse{Generation: uint64(count), NextPageToken: next}
				for i := start; i < min(start+7, count); i++ {
					source, _, _ := desiredRecords()
					source.GetSource().Id = fmt.Sprintf("source-%03d", i)
					value := structpb.NewStringValue(strings.Repeat("x", 64<<10))
					source.GetSource().Spec.Kind = &contractpb.SourceSpec_External{External: &contractpb.ComponentConfiguration{
						Configuration: &structpb.Struct{Fields: map[string]*structpb.Value{"configuration": value}},
					}}
					size := len(value.GetStringValue()) + (64 << 10) - proto.Size(source)
					target := 64 << 10
					if i == count-1 {
						size += extra
						target += extra
					}
					value.Kind = &structpb.Value_StringValue{StringValue: strings.Repeat("x", size)}
					require.Equal(t, target, proto.Size(source))
					response.Records = append(response.Records, source)
				}
				steps = append(steps, desiredReadStep{request: desiredList(registrypb.RESOURCE_KIND_SOURCE, token), response: response})
			}
			if extra == 0 {
				for _, kind := range []registrypb.ResourceKind{registrypb.RESOURCE_KIND_QUERY, registrypb.RESOURCE_KIND_WATCH} {
					steps = append(steps, desiredReadStep{request: desiredList(kind, ""), response: &registrypb.ReadResponse{Generation: uint64(count)}})
				}
			}
			client := &desiredTestClient{t: t, steps: steps}
			view, err := ReadDesiredView(testcontext.For(t), client, "00000000-0000-4000-8000-000000000001", "ns")
			if extra == 0 {
				require.NoError(t, err)
				require.Len(t, view.revisions, 32)
			} else {
				require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
				require.Nil(t, view)
			}
			require.Equal(t, len(steps), client.calls)
		})
	}
}
