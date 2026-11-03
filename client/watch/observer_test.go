package watch

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func observationFixture(now time.Time) (*watchservice.ObserveRequest, *watchservice.ObserveResponse) {
	request := &watchservice.ObserveRequest{
		NamespaceId: "00000000-0000-4000-8000-000000000001", Generation: 9,
		Owner: &watchservice.Owner{
			Epoch: 3, StorageHostId: "10000000-0000-4000-8000-000000000001", ProcessBootId: "20000000-0000-4000-8000-000000000001",
		},
	}
	response := &watchservice.ObserveResponse{NamespaceId: request.NamespaceId, Generation: request.Generation, Owner: common.CloneProto(request.Owner)}
	for _, kind := range []watchservice.ResourceKind{watchservice.RESOURCE_KIND_SOURCE, watchservice.RESOURCE_KIND_QUERY, watchservice.RESOURCE_KIND_WATCH} {
		target := &watchservice.Target{Kind: kind, Id: "same-id", Revision: 7, Incarnation: "4"}
		request.Targets = append(request.Targets, target)
		response.Observations = append(response.Observations, &watchservice.ResourceObservation{
			Target: common.CloneProto(target), Status: &contractpb.ResourceStatus{
				State: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING, ObservedRevision: 7,
				UpdatedAt: timestamppb.New(now.Add(-time.Second)),
			},
		})
	}
	return request, response
}

func TestObservationResponseValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*watchservice.ObserveResponse)
	}{
		{"namespace", func(r *watchservice.ObserveResponse) { r.NamespaceId = "other" }},
		{"generation", func(r *watchservice.ObserveResponse) { r.Generation++ }},
		{"owner", func(r *watchservice.ObserveResponse) { r.Owner.Epoch++ }},
		{"omitted target", func(r *watchservice.ObserveResponse) { r.Observations = r.Observations[:2] }},
		{"duplicate target", func(r *watchservice.ObserveResponse) {
			r.Observations[1].Target = common.CloneProto(r.Observations[0].Target)
		}},
		{"wrong revision", func(r *watchservice.ObserveResponse) { r.Observations[0].Status.ObservedRevision++ }},
		{"wrong incarnation", func(r *watchservice.ObserveResponse) { r.Observations[0].Target.Incarnation = "next" }},
		{"unknown state", func(r *watchservice.ObserveResponse) { r.Observations[0].Status.State = 99 }},
		{"missing time", func(r *watchservice.ObserveResponse) { r.Observations[0].Status.UpdatedAt = nil }},
		{"invalid time", func(r *watchservice.ObserveResponse) { r.Observations[0].Status.UpdatedAt.Seconds = 1 << 60 }},
		{"stale time", func(r *watchservice.ObserveResponse) { r.Observations[0].Status.UpdatedAt.Seconds -= 5 }},
		{"future time", func(r *watchservice.ObserveResponse) { r.Observations[0].Status.UpdatedAt.Seconds += 5 }},
		{"oversized status", func(r *watchservice.ObserveResponse) {
			r.Observations[0].Status.Message = strings.Repeat("x", MaxStatusBytes)
		}},
		{"oversized response", func(r *watchservice.ObserveResponse) { r.Observations[0].Status.Message = strings.Repeat("x", 1<<20) }},
		{"invented accepted output", func(r *watchservice.ObserveResponse) {
			r.Observations[0].Status.AcceptedOutput = &contractpb.OutputIdentity{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			request, response := observationFixture(now)
			tc.change(response)
			statuses, err := observationStatuses(request, response, now)
			require.Equal(t, codes.DataLoss, serviceerror.ToStatus(err).Code())
			require.Nil(t, statuses)
		})
	}
	t.Run("complete detached observations", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		request, response := observationFixture(now)
		response.Observations[1].Status = nil
		statuses, err := observationStatuses(request, response, now)
		require.NoError(t, err)
		require.Len(t, statuses, 3)
		protorequire.ProtoEqual(t, response.Observations[0].Status, statuses[0])
		protorequire.ProtoEqual(t, &contractpb.ResourceStatus{Message: "Runtime has not observed this resource revision"}, statuses[1])
		statuses[0].ObservedRevision++
		require.Equal(t, uint64(7), response.Observations[0].Status.ObservedRevision)
	})
}

type observationAssignmentClient struct {
	assignmentpb.AssignmentServiceClient
	describe func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error)
}

func (c observationAssignmentClient) Describe(ctx context.Context, request *assignmentpb.DescribeRequest, _ ...grpc.CallOption) (*assignmentpb.State, error) {
	return c.describe(ctx, request)
}

type observationTestServer struct {
	watchservice.UnimplementedWatchServiceServer
	response *watchservice.ObserveResponse
	err      error
	wait     bool
	requests chan *watchservice.ObserveRequest
}

func (s *observationTestServer) Observe(ctx context.Context, request *watchservice.ObserveRequest) (*watchservice.ObserveResponse, error) {
	s.requests <- common.CloneProto(request)
	if s.wait {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return common.CloneProto(s.response), s.err
}

func TestObserverFencesOneBoundedPageThroughRealRPC(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		count   int
		change  bool
		seal    bool
		wait    bool
		rpcFail bool
		code    codes.Code
	}{
		{name: "one resource", count: 1},
		{name: "full public page", count: 200},
		{name: "owner changed in flight", count: 1, change: true, code: codes.Unavailable},
		{name: "owner sealed in flight", count: 1, seal: true, code: codes.Unavailable},
		{name: "membership points to nonowner", count: 1, rpcFail: true, code: codes.Unavailable},
		{name: "observation deadline", count: 1, wait: true, code: codes.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request, response := observationFixture(time.Now())
			request.Targets, response.Observations = nil, nil
			snapshot := &registrypb.ReadResponse{Generation: request.Generation}
			for i := range tc.count {
				record := &registrypb.Record{Incarnation: "4", Resource: &registrypb.Record_Source{Source: &contractpb.Source{
					Id: fmt.Sprintf("source-%03d", i), Revision: 7,
				}}}
				target, err := RecordTarget(record)
				require.NoError(t, err)
				snapshot.Records, request.Targets = append(snapshot.Records, record), append(request.Targets, target)
				response.Observations = append(response.Observations, &watchservice.ResourceObservation{
					Target: common.CloneProto(target),
					Status: &contractpb.ResourceStatus{State: contractpb.LifecycleState_LIFECYCLE_STATE_RUNNING,
						ObservedRevision: 7, UpdatedAt: timestamppb.Now()},
				})
			}
			owner := &assignmentpb.State{Version: 3, Phase: &assignmentpb.State_Active{Active: &assignmentpb.Active{Grant: &assignmentpb.Grant{
				NamespaceId: request.NamespaceId, InstanceId: request.NamespaceId, Epoch: request.Owner.Epoch,
				StorageHostId: request.Owner.StorageHostId, ProcessBootId: request.Owner.ProcessBootId,
			}}}}
			calls := 0
			assignments := observationAssignmentClient{describe: func(_ context.Context, r *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
				protorequire.ProtoEqual(t, &assignmentpb.DescribeRequest{
					NamespaceId: request.NamespaceId, ExecutionId: watchassignment.BusinessID,
				}, r)
				calls++
				state := common.CloneProto(owner)
				if calls == 2 && tc.change {
					state.GetActive().Grant.Epoch++
				}
				if calls == 2 && tc.seal {
					state.Phase = &assignmentpb.State_Sealed{Sealed: &assignmentpb.Sealed{}}
				}
				return state, nil
			}}
			server := &observationTestServer{response: response, wait: tc.wait, requests: make(chan *watchservice.ObserveRequest, 1)}
			if tc.rpcFail {
				server.err = status.Error(codes.Unavailable, "candidate does not own this namespace")
			}
			listener := nettest.NewListener(nettest.NewPipe())
			rpcServer := grpc.NewServer()
			watchservice.RegisterWatchServiceServer(rpcServer, server)
			done := make(chan error, 1)
			go func() { done <- rpcServer.Serve(listener) }()
			defer func() {
				rpcServer.Stop()
				require.NoError(t, await.Rcv(t, done))
			}()
			conn := nettest.NewRPCFactory(listener).CreateWatchGRPCConnection("passthrough:///watch")
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			controller := gomock.NewController(t)
			monitor, resolver := membership.NewMockMonitor(controller), membership.NewMockServiceResolver(controller)
			monitor.EXPECT().GetResolver(primitives.WatchService).Return(resolver, nil)
			resolver.EXPECT().Lookup(request.NamespaceId).Return(membership.NewHostInfoFromAddress("watch-candidate:7239"), nil)
			factory := common.NewMockRPCFactory(controller)
			factory.EXPECT().CreateWatchGRPCConnection("watch-candidate:7239").Return(conn)
			observer := NewObserver(assignments, monitor, factory)
			statuses, err := observer.Observe(t.Context(), request.NamespaceId, snapshot)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, request, await.Rcv(t, server.requests))
			if tc.code != codes.OK {
				require.Nil(t, statuses)
			} else {
				require.Len(t, statuses, tc.count)
				for i, entry := range response.Observations {
					protorequire.ProtoEqual(t, entry.Status, statuses[i])
				}
			}
			want := 2
			if tc.rpcFail || tc.wait {
				want = 1
			}
			require.Equal(t, want, calls)
		})
	}
}

func TestObserverWithoutRuntimeDoesNotDialOrAcquire(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		state *assignmentpb.State
		err   error
		code  codes.Code
	}{
		{name: "no participation", err: serviceerror.NewNotFound("no assignment")},
		{name: "sealed owner", state: &assignmentpb.State{Version: 1, Phase: &assignmentpb.State_Sealed{Sealed: &assignmentpb.Sealed{}}}},
		{name: "corrupt assignment", state: &assignmentpb.State{Version: 1}, code: codes.DataLoss},
		{name: "read denied", err: serviceerror.NewPermissionDenied("denied", ""), code: codes.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observer := NewObserver(observationAssignmentClient{describe: func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
				return tc.state, tc.err
			}}, nil, nil)
			statuses, err := observer.Observe(t.Context(), "ns", &registrypb.ReadResponse{Generation: 1, Records: []*registrypb.Record{{}}})
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			require.Nil(t, statuses)
		})
	}
}

func TestObserverBoundsBeforeAssignment(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		snapshot *registrypb.ReadResponse
		code     codes.Code
	}{
		{name: "empty page", snapshot: &registrypb.ReadResponse{}},
		{name: "missing registry cut", snapshot: &registrypb.ReadResponse{Records: []*registrypb.Record{{}}}, code: codes.DataLoss},
		{name: "oversized registry page", snapshot: &registrypb.ReadResponse{Generation: 1, Records: make([]*registrypb.Record, 201)}, code: codes.DataLoss},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observer := NewObserver(nil, nil, nil)
			statuses, err := observer.Observe(t.Context(), "ns", tc.snapshot)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			require.Nil(t, statuses)
		})
	}
}
