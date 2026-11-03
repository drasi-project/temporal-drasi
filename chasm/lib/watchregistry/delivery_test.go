package watchregistry

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watch"
	watchpb "go.temporal.io/server/chasm/lib/watch/gen/watchpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type deliveryControlClient struct {
	watchpb.ControlServiceClient
	describe func(*watchpb.DescribeExecutionRequest) (*watchpb.ExecutionResponse, error)
}

func (c deliveryControlClient) DescribeExecution(_ context.Context, request *watchpb.DescribeExecutionRequest, _ ...grpc.CallOption) (*watchpb.ExecutionResponse, error) {
	return c.describe(request)
}

func TestRegistryWatchHistoryRetainsLifetimes(t *testing.T) {
	t.Parallel()
	env := newEnvironment(t, DefaultLimits())
	_, err := env.mutate(env.createSource("source", "source"))
	require.NoError(t, err)
	_, err = env.mutate(env.createQuery("query", "source", 1))
	require.NoError(t, err)
	_, err = env.mutate(env.createWatch("watch", watchSpec("query", 1)))
	require.NoError(t, err)
	for _, request := range []*registrypb.MutateRequest{
		{Operation: &registrypb.MutateRequest_PauseWatch{PauseWatch: env.watchOperation("watch", 1, "pause")}},
		{Operation: &registrypb.MutateRequest_ResumeWatch{ResumeWatch: env.watchOperation("watch", 2, "resume")}},
		{Operation: &registrypb.MutateRequest_UpdateWatch{UpdateWatch: &contractpb.UpdateWatchRequest{
			Namespace: env.namespace, Id: "watch", ExpectedRevision: 3, RequestId: "replace", Spec: watchSpec("query", 1),
		}}},
		{Operation: &registrypb.MutateRequest_ResetWatch{ResetWatch: &contractpb.ResetWatchRequest{
			Namespace: env.namespace, Id: "watch", ExpectedRevision: 4, RequestId: "reset",
		}}},
		{Operation: &registrypb.MutateRequest_DeleteWatch{DeleteWatch: env.watchOperation("watch", 5, "delete")}},
		{Operation: &registrypb.MutateRequest_CreateWatch{CreateWatch: &contractpb.CreateWatchRequest{
			Namespace: env.namespace, Id: "watch", RequestId: "recreate", Spec: watchSpec("query", 1),
		}}},
	} {
		_, err := env.mutate(request)
		require.NoError(t, err)
	}
	first, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_WatchHistory{
		WatchHistory: &registrypb.WatchHistoryLookup{Id: "watch", Limit: 2},
	}})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &registrypb.ReadResponse{
		Generation: env.state(t).Generation, MoreLifetimes: true,
		WatchLifetimes: []*registrypb.WatchLifetime{
			{Incarnation: "1", FirstRevision: 1, LastRevision: 3},
			{Incarnation: "4", FirstRevision: 4, LastRevision: 4},
		},
	}, first)
	lookup := &registrypb.WatchHistoryLookup{Id: "watch", Limit: 2, Generation: first.Generation, AfterRevision: 4}
	second, err := env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_WatchHistory{WatchHistory: lookup}})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &registrypb.ReadResponse{
		Generation: first.Generation,
		WatchLifetimes: []*registrypb.WatchLifetime{
			{Incarnation: "5", FirstRevision: 5, LastRevision: 6},
			{Incarnation: "7", FirstRevision: 7, LastRevision: 7},
		},
	}, second)
	_, err = env.mutate(env.createWatch("another", watchSpec("query", 1)))
	require.NoError(t, err)
	_, err = env.read(&registrypb.ReadRequest{Operation: &registrypb.ReadRequest_WatchHistory{WatchHistory: lookup}})
	require.ErrorContains(t, err, "restart the list")
}

func deliveryFixture(revision, sequence uint64, ordinal uint32) *watchpb.Intent {
	output := &contractpb.OutputIdentity{
		QueryRevision: 2, Incarnation: "native-query", Sequence: sequence, DiffOrdinal: ordinal, RowIdentity: "row",
	}
	return &watchpb.Intent{
		ActionId: watch.ActionID(watch.ActionIdentity{
			Namespace: "test", WatchID: "watch", Revision: revision, Incarnation: output.Incarnation,
			Sequence: sequence, Ordinal: uint64(ordinal), RowKey: output.RowIdentity,
		}),
		WorkflowId: "target", RunId: "resolved-run", Status: watchpb.DELIVERY_STATUS_DELIVERED,
		Output: output, UpdatedAt: timestamppb.New(time.Unix(1_000, 0)), Attempts: 1,
		Request: []byte("private-request"),
	}
}

func TestFrontendDeliveryKeysetPaginationAcrossLifetimes(t *testing.T) {
	t.Parallel()
	lifetimes := []*registrypb.WatchLifetime{
		{Incarnation: "1", FirstRevision: 1, LastRevision: 3},
		{Incarnation: "4", FirstRevision: 4, LastRevision: 4},
	}
	controls := map[string]*watchpb.ExecutionResponse{}
	for i, lifetime := range lifetimes {
		id, err := watch.ExecutionID("00000000-0000-4000-8000-000000000001", "watch", lifetime.Incarnation)
		require.NoError(t, err)
		definition, err := proto.Marshal(&contractpb.Watch{
			Spec: &contractpb.WatchSpec{Query: &contractpb.ResourceRef{Id: "query", Revision: 2}},
		})
		require.NoError(t, err)
		controls[id] = &watchpb.ExecutionResponse{
			State: &watchpb.State{
				Namespace: "test", WatchId: "watch", CreationRevision: lifetime.LastRevision, Incarnation: "native-query", Definition: definition,
			},
			Deliveries: []*watchpb.Intent{
				deliveryFixture(lifetime.LastRevision, uint64(i+1), 2),
				deliveryFixture(lifetime.LastRevision, uint64(i+1), 0),
			},
		}
	}
	var generation uint64 = 9
	h := frontendForTest(t, frontendRegistryClient{read: func(_ context.Context, request *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
		require.Equal(t, "00000000-0000-4000-8000-000000000001", request.NamespaceId)
		require.Equal(t, BusinessID, request.ExecutionId)
		lookup := request.GetWatchHistory()
		require.NotNil(t, lookup)
		require.Equal(t, "watch", lookup.Id)
		response := &registrypb.ReadResponse{Generation: generation}
		for i, lifetime := range lifetimes {
			if lifetime.FirstRevision > lookup.AfterRevision {
				response.WatchLifetimes = []*registrypb.WatchLifetime{common.CloneProto(lifetime)}
				response.MoreLifetimes = i+1 < len(lifetimes)
				break
			}
		}
		return response, nil
	}})
	h.control = deliveryControlClient{describe: func(request *watchpb.DescribeExecutionRequest) (*watchpb.ExecutionResponse, error) {
		require.Equal(t, "00000000-0000-4000-8000-000000000001", request.NamespaceId)
		return common.CloneProto(controls[request.ExecutionId]), nil
	}}
	request := &contractpb.ListDeliveriesRequest{Namespace: "test", Id: "watch", PageSize: 1}
	var outputs []*contractpb.OutputIdentity
	var revisions []uint64
	for range 4 {
		page, err := h.ListDeliveries(t.Context(), request)
		require.NoError(t, err)
		require.Len(t, page.Deliveries, 1)
		delivery := page.Deliveries[0]
		require.Equal(t, contractpb.DeliveryState_DELIVERY_STATE_DELIVERED, delivery.State)
		require.Equal(t, "target", delivery.Execution.WorkflowId)
		require.Equal(t, "resolved-run", delivery.Execution.RunId)
		encoded, err := proto.Marshal(page)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "private-request")
		outputs = append(outputs, delivery.Output)
		revisions = append(revisions, delivery.Watch.Revision)
		request.PageToken = page.NextPageToken
	}
	require.Empty(t, request.PageToken)
	require.Equal(t, []uint64{3, 3, 4, 4}, revisions)
	require.Equal(t, []uint64{1, 1, 2, 2}, []uint64{outputs[0].Sequence, outputs[1].Sequence, outputs[2].Sequence, outputs[3].Sequence})
	require.Equal(t, []uint32{0, 2, 0, 2}, []uint32{outputs[0].DiffOrdinal, outputs[1].DiffOrdinal, outputs[2].DiffOrdinal, outputs[3].DiffOrdinal})

	first, err := h.ListDeliveries(t.Context(), request)
	require.NoError(t, err)
	request.PageToken = first.NextPageToken
	generation++
	_, err = h.ListDeliveries(t.Context(), request)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
}

func TestFrontendDeliveryTokensAreScoped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		cursor *registrypb.DeliveryCursor
	}{
		{"another namespace", &registrypb.DeliveryCursor{NamespaceId: "other", WatchId: "watch", Generation: 1, LifetimeRevision: 1}},
		{"another Watch", &registrypb.DeliveryCursor{NamespaceId: "namespace-id", WatchId: "other", Generation: 1, LifetimeRevision: 1}},
		{"missing generation", &registrypb.DeliveryCursor{NamespaceId: "namespace-id", WatchId: "watch", LifetimeRevision: 1}},
		{"missing lifetime", &registrypb.DeliveryCursor{NamespaceId: "namespace-id", WatchId: "watch", Generation: 1, WithinLifetime: true}},
		{"inconsistent position", &registrypb.DeliveryCursor{NamespaceId: "namespace-id", WatchId: "watch", Generation: 1, LifetimeRevision: 1, Sequence: 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded, err := proto.Marshal(tc.cursor)
			require.NoError(t, err)
			_, err = deliveryCursor(&contractpb.ListDeliveriesRequest{
				Id: "watch", PageToken: base64.RawURLEncoding.EncodeToString(encoded),
			}, "namespace-id")
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
		})
	}
}

func TestFrontendDeliveryPageByteLimits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		rowBytes int
		pages    int
		code     codes.Code
	}{
		{"one page", 64, 1, codes.OK},
		{"byte-bounded continuation", 300 * 1024, 2, codes.OK},
		{"single delivery exceeds limit", MaxPageBytes, 0, codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			definition, err := proto.Marshal(&contractpb.Watch{
				Spec: &contractpb.WatchSpec{Query: &contractpb.ResourceRef{Id: "query", Revision: 2}},
			})
			require.NoError(t, err)
			control := &watchpb.ExecutionResponse{
				State: &watchpb.State{
					Namespace: "test", WatchId: "watch", CreationRevision: 1,
					Incarnation: "native-query", Definition: definition,
				},
			}
			for sequence := uint64(1); sequence <= 2; sequence++ {
				intent := deliveryFixture(1, sequence, 0)
				intent.Output.RowIdentity = strings.Repeat("x", tc.rowBytes)
				intent.ActionId = watch.ActionID(watch.ActionIdentity{
					Namespace: "test", WatchID: "watch", Revision: 1, Incarnation: intent.Output.Incarnation,
					Sequence: sequence, RowKey: intent.Output.RowIdentity,
				})
				control.Deliveries = append(control.Deliveries, intent)
			}
			h := frontendForTest(t, frontendRegistryClient{read: func(context.Context, *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
				return &registrypb.ReadResponse{
					Generation: 1, WatchLifetimes: []*registrypb.WatchLifetime{{Incarnation: "1", FirstRevision: 1, LastRevision: 1}},
				}, nil
			}})
			h.control = deliveryControlClient{describe: func(*watchpb.DescribeExecutionRequest) (*watchpb.ExecutionResponse, error) {
				return common.CloneProto(control), nil
			}}
			request := &contractpb.ListDeliveriesRequest{Namespace: "test", Id: "watch"}
			var sequences []uint64
			for pageNumber := range max(1, tc.pages) {
				page, err := h.ListDeliveries(t.Context(), request)
				require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
				if tc.code != codes.OK {
					require.Nil(t, page)
					return
				}
				require.LessOrEqual(t, proto.Size(page), MaxPageBytes)
				for _, delivery := range page.Deliveries {
					sequences = append(sequences, delivery.Output.Sequence)
				}
				require.Equal(t, pageNumber+1 < tc.pages, page.NextPageToken != "")
				request.PageToken = page.NextPageToken
			}
			require.Equal(t, []uint64{1, 2}, sequences)
		})
	}
}

func TestFrontendDeliveryBoundsEmptyLifetimeScan(t *testing.T) {
	t.Parallel()
	definition, err := proto.Marshal(&contractpb.Watch{
		Spec: &contractpb.WatchSpec{Query: &contractpb.ResourceRef{Id: "query", Revision: 2}},
	})
	require.NoError(t, err)
	lastID, err := watch.ExecutionID("00000000-0000-4000-8000-000000000001", "watch", "9")
	require.NoError(t, err)
	reads, descriptions := 0, 0
	h := frontendForTest(t, frontendRegistryClient{read: func(_ context.Context, request *registrypb.ReadRequest) (*registrypb.ReadResponse, error) {
		reads++
		revision := request.GetWatchHistory().AfterRevision + 1
		require.LessOrEqual(t, revision, uint64(9))
		return &registrypb.ReadResponse{
			Generation: 1, MoreLifetimes: revision < 9,
			WatchLifetimes: []*registrypb.WatchLifetime{{
				Incarnation: fmt.Sprint(revision), FirstRevision: revision, LastRevision: revision,
			}},
		}, nil
	}})
	h.control = deliveryControlClient{describe: func(request *watchpb.DescribeExecutionRequest) (*watchpb.ExecutionResponse, error) {
		descriptions++
		if request.ExecutionId != lastID {
			return nil, serviceerror.NewNotFound("retained control expired")
		}
		return &watchpb.ExecutionResponse{
			State: &watchpb.State{
				Namespace: "test", WatchId: "watch", CreationRevision: 9, Incarnation: "native-query", Definition: definition,
			},
			Deliveries: []*watchpb.Intent{deliveryFixture(9, 1, 0)},
		}, nil
	}}
	request := &contractpb.ListDeliveriesRequest{Namespace: "test", Id: "watch"}
	first, err := h.ListDeliveries(t.Context(), request)
	require.NoError(t, err)
	require.Empty(t, first.Deliveries)
	require.NotEmpty(t, first.NextPageToken)
	require.Equal(t, 8, reads)
	require.Equal(t, 8, descriptions)
	request.PageToken = first.NextPageToken
	last, err := h.ListDeliveries(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, last.Deliveries, 1)
	require.Equal(t, uint64(9), last.Deliveries[0].Watch.Revision)
	require.Empty(t, last.NextPageToken)
	require.Equal(t, 9, reads)
	require.Equal(t, 9, descriptions)
}
