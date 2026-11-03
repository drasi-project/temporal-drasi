package watch

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func pluginObservationFixture(now time.Time, member int, count int) (*watchservice.ObservePluginsRequest, *watchservice.ObservePluginsResponse) {
	request := &watchservice.ObservePluginsRequest{Generation: 7}
	response := &watchservice.ObservePluginsResponse{
		Generation:    request.Generation,
		StorageHostId: fmt.Sprintf("10000000-0000-4000-8000-%012d", member),
		ProcessBootId: fmt.Sprintf("20000000-0000-4000-8000-%012d", member),
	}
	for i := range count {
		digest := fmt.Sprintf("sha256:%064x", i+1)
		request.Digests = append(request.Digests, digest)
		response.Observations = append(response.Observations, &watchservice.PluginObservation{
			Digest: digest, Status: &contractpb.PluginHostStatus{
				HostId: response.StorageHostId, State: contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED,
				UpdatedAt: timestamppb.New(now), VerifiedSignerIdentity: "subject (issuer: issuer)",
			},
		})
	}
	return request, response
}

func TestPluginObservationStatuses(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*watchservice.ObservePluginsResponse)
	}{
		{"wrong generation", func(r *watchservice.ObservePluginsResponse) { r.Generation++ }},
		{"empty volume", func(r *watchservice.ObservePluginsResponse) { r.StorageHostId = "" }},
		{"zero volume", func(r *watchservice.ObservePluginsResponse) { r.StorageHostId = "00000000-0000-0000-0000-000000000000" }},
		{"invalid boot", func(r *watchservice.ObservePluginsResponse) { r.ProcessBootId = "not-a-boot" }},
		{"noncanonical boot", func(r *watchservice.ObservePluginsResponse) {
			r.ProcessBootId = strings.ReplaceAll(r.ProcessBootId, "-", "")
		}},
		{"missing artifact", func(r *watchservice.ObservePluginsResponse) { r.Observations = r.Observations[:1] }},
		{"nil artifact", func(r *watchservice.ObservePluginsResponse) { r.Observations[0] = nil }},
		{"reordered artifact", func(r *watchservice.ObservePluginsResponse) { slices.Reverse(r.Observations) }},
		{"duplicate artifact", func(r *watchservice.ObservePluginsResponse) {
			r.Observations[1] = common.CloneProto(r.Observations[0])
		}},
		{"missing status", func(r *watchservice.ObservePluginsResponse) { r.Observations[0].Status = nil }},
		{"wrong status volume", func(r *watchservice.ObservePluginsResponse) { r.Observations[0].Status.HostId = "other" }},
		{"unknown state", func(r *watchservice.ObservePluginsResponse) { r.Observations[0].Status.State = 99 }},
		{"missing signer", func(r *watchservice.ObservePluginsResponse) { r.Observations[0].Status.VerifiedSignerIdentity = "" }},
		{"missing time", func(r *watchservice.ObservePluginsResponse) { r.Observations[0].Status.UpdatedAt = nil }},
		{"invalid time", func(r *watchservice.ObservePluginsResponse) { r.Observations[0].Status.UpdatedAt.Seconds = 1 << 60 }},
		{"stale time", func(r *watchservice.ObservePluginsResponse) { r.Observations[0].Status.UpdatedAt.Seconds -= 121 }},
		{"future time", func(r *watchservice.ObservePluginsResponse) { r.Observations[0].Status.UpdatedAt.Seconds += 3 }},
		{"unknown cannot claim signer", func(r *watchservice.ObservePluginsResponse) {
			r.Observations[0].Status.State = contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED
			r.Observations[0].Status.Message = "not known"
		}},
		{"unknown requires diagnostic", func(r *watchservice.ObservePluginsResponse) {
			r.Observations[0].Status = &contractpb.PluginHostStatus{HostId: r.StorageHostId}
		}},
		{"oversized status", func(r *watchservice.ObservePluginsResponse) {
			r.Observations[0].Status.Message = strings.Repeat("x", MaxPluginHostStatusBytes)
		}},
		{"oversized envelope", func(r *watchservice.ObservePluginsResponse) {
			r.Observations[0].Status.Message = strings.Repeat("x", 1<<20)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			request, response := pluginObservationFixture(now, 1, 2)
			test.change(response)
			statuses, err := pluginObservationStatuses(request, response, "member:7239", now)
			require.Equal(t, codes.DataLoss, serviceerror.ToStatus(err).Code())
			require.Nil(t, statuses)
		})
	}
	t.Run("detached public identities and explicit unknown", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		request, response := pluginObservationFixture(now, 1, 2)
		response.Observations[1].Status = &contractpb.PluginHostStatus{HostId: response.StorageHostId, Message: "not yet observed"}
		statuses, err := pluginObservationStatuses(request, response, "member:7239", now)
		require.NoError(t, err)
		protorequire.ProtoSliceEqual(t, []*contractpb.PluginHostStatus{
			{HostId: "member:7239", State: contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED,
				UpdatedAt: timestamppb.New(now), VerifiedSignerIdentity: "subject (issuer: issuer)"},
			{HostId: "member:7239", Message: "not yet observed"},
		}, statuses)
		statuses[0].VerifiedSignerIdentity = "changed"
		require.Equal(t, "subject (issuer: issuer)", response.Observations[0].Status.VerifiedSignerIdentity)
		require.Equal(t, response.StorageHostId, response.Observations[0].Status.HostId)
	})
	t.Run("nil envelope", func(t *testing.T) {
		t.Parallel()
		request, _ := pluginObservationFixture(time.Now(), 1, 1)
		statuses, err := pluginObservationStatuses(request, nil, "member:7239", time.Now())
		require.Nil(t, statuses)
		require.Equal(t, codes.DataLoss, serviceerror.ToStatus(err).Code())
	})
}

type pluginObservationTestServer struct {
	watchservice.UnimplementedWatchServiceServer
	observe func(context.Context, *watchservice.ObservePluginsRequest) (*watchservice.ObservePluginsResponse, error)
}

func (s *pluginObservationTestServer) ObservePlugins(ctx context.Context, request *watchservice.ObservePluginsRequest) (*watchservice.ObservePluginsResponse, error) {
	return s.observe(ctx, request)
}

func pluginObservationRPC(t *testing.T, server *pluginObservationTestServer) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := nettest.NewListener(nettest.NewPipe())
	rpcServer := grpc.NewServer()
	watchservice.RegisterWatchServiceServer(rpcServer, server)
	done := make(chan error, 1)
	go func() { done <- rpcServer.Serve(listener) }()
	conn := nettest.NewRPCFactory(listener).CreateWatchGRPCConnection("passthrough:///watch")
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return conn, func() {
		rpcServer.Stop()
		require.NoError(t, await.Rcv(t, done))
	}
}

func TestPluginObserverIncludesEveryMemberThroughRPC(t *testing.T) {
	t.Parallel()
	const (
		loaded  = contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED
		unknown = contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED
	)
	for _, test := range []struct {
		name   string
		count  int
		change func(*watchservice.ObservePluginsResponse)
		final  []membership.HostInfo
		fail   bool
		want   []contractpb.PluginHostState
	}{
		{name: "both members loaded", count: 2, want: []contractpb.PluginHostState{loaded, loaded}},
		{name: "one full artifact page", count: 200, want: []contractpb.PluginHostState{loaded, loaded}},
		{name: "unreachable member retained", count: 2, fail: true, want: []contractpb.PluginHostState{loaded, unknown}},
		{name: "wrong generation remains unknown", count: 2, change: func(r *watchservice.ObservePluginsResponse) { r.Generation++ }, want: []contractpb.PluginHostState{loaded, unknown}},
		{name: "stale member remains unknown", count: 2, change: func(r *watchservice.ObservePluginsResponse) {
			r.Observations[0].Status.UpdatedAt = timestamppb.New(time.Now().Add(-3 * time.Minute))
		}, want: []contractpb.PluginHostState{loaded, unknown}},
		{name: "duplicate volume cannot load", count: 2, change: func(r *watchservice.ObservePluginsResponse) {
			r.StorageHostId = "10000000-0000-4000-8000-000000000001"
			for _, entry := range r.Observations {
				entry.Status.HostId = r.StorageHostId
			}
		}, want: []contractpb.PluginHostState{unknown, unknown}},
		{name: "duplicate boot cannot load", count: 2, change: func(r *watchservice.ObservePluginsResponse) {
			r.ProcessBootId = "20000000-0000-4000-8000-000000000001"
		}, want: []contractpb.PluginHostState{unknown, unknown}},
		{name: "new member invalidates cut", count: 2,
			final: []membership.HostInfo{membership.NewHostInfoFromAddress("member-a:7239"), membership.NewHostInfoFromAddress("member-b:7239"), membership.NewHostInfoFromAddress("member-c:7239")},
			want:  []contractpb.PluginHostState{unknown, unknown, unknown}},
		{name: "removed member retained as unknown", count: 2,
			final: []membership.HostInfo{membership.NewHostInfoFromAddress("member-b:7239")},
			want:  []contractpb.PluginHostState{unknown, unknown}},
		{name: "all members disappeared", count: 2, final: []membership.HostInfo{}, want: []contractpb.PluginHostState{unknown, unknown}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request, first := pluginObservationFixture(time.Now(), 1, test.count)
			_, second := pluginObservationFixture(time.Now(), 2, test.count)
			if test.change != nil {
				test.change(second)
			}
			responses := []*watchservice.ObservePluginsResponse{first, second}
			members := []membership.HostInfo{membership.NewHostInfoFromAddress("member-a:7239"), membership.NewHostInfoFromAddress("member-b:7239")}
			controller := gomock.NewController(t)
			monitor, resolver := membership.NewMockMonitor(controller), membership.NewMockServiceResolver(controller)
			monitor.EXPECT().GetResolver(primitives.WatchService).Return(resolver, nil)
			final := test.final
			if final == nil {
				final = []membership.HostInfo{members[1], members[0]}
			}
			gomock.InOrder(resolver.EXPECT().Members().Return(members), resolver.EXPECT().Members().Return(final))
			factory := common.NewMockRPCFactory(controller)
			requests := make(chan *watchservice.ObservePluginsRequest, 2)
			for i, member := range members {
				server := &pluginObservationTestServer{observe: func(_ context.Context, r *watchservice.ObservePluginsRequest) (*watchservice.ObservePluginsResponse, error) {
					requests <- common.CloneProto(r)
					if i == 1 && test.fail {
						return nil, serviceerror.NewUnavailable("private-untrusted-diagnostic")
					}
					return common.CloneProto(responses[i]), nil
				}}
				conn, stop := pluginObservationRPC(t, server)
				defer stop()
				factory.EXPECT().CreateWatchGRPCConnection(member.GetAddress()).Return(conn)
			}
			observer := NewObserver(nil, monitor, factory)
			result, err := observer.ObservePlugins(t.Context(), request.Generation, request.Digests)
			require.NoError(t, err)
			for range members {
				protorequire.ProtoEqual(t, request, await.Rcv(t, requests))
			}
			require.Len(t, result, test.count)
			for _, digest := range request.Digests {
				statuses := result[digest]
				require.Len(t, statuses, len(test.want))
				for i, want := range test.want {
					require.Equal(t, fmt.Sprintf("member-%c:7239", 'a'+i), statuses[i].HostId)
					require.Equal(t, want, statuses[i].State)
					if want == contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED {
						require.NotEmpty(t, statuses[i].Message)
						require.Empty(t, statuses[i].VerifiedSignerIdentity)
						require.Nil(t, statuses[i].UpdatedAt)
						require.NotContains(t, statuses[i].Message, "private-untrusted-diagnostic")
					}
				}
			}
		})
	}
}

func TestPluginObserverBoundsJoinedRPCsAcrossRequests(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	active, maximum, completed := 0, 0, 0
	started, release := make(chan struct{}, 32), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	server := &pluginObservationTestServer{observe: func(ctx context.Context, _ *watchservice.ObservePluginsRequest) (*watchservice.ObservePluginsResponse, error) {
		mu.Lock()
		active++
		maximum = max(maximum, active)
		mu.Unlock()
		defer func() {
			mu.Lock()
			active--
			completed++
			mu.Unlock()
		}()
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, serviceerror.NewUnavailable("deliberate member failure")
	}}
	conn, stop := pluginObservationRPC(t, server)
	defer stop()
	defer unblock()
	members := make([]membership.HostInfo, 9)
	for i := range members {
		members[i] = membership.NewHostInfoFromAddress(fmt.Sprintf("member-%02d:7239", i))
	}
	controller := gomock.NewController(t)
	monitor, resolver := membership.NewMockMonitor(controller), membership.NewMockServiceResolver(controller)
	monitor.EXPECT().GetResolver(primitives.WatchService).Return(resolver, nil).Times(2)
	resolver.EXPECT().Members().Return(members).Times(4)
	factory := common.NewMockRPCFactory(controller)
	factory.EXPECT().CreateWatchGRPCConnection(gomock.Any()).Return(conn).Times(18)
	observer := NewObserver(nil, monitor, factory)
	request, _ := pluginObservationFixture(time.Now(), 1, 1)
	type result struct {
		statuses map[string][]*contractpb.PluginHostStatus
		err      error
	}
	done := make(chan result, 2)
	for range 2 {
		go func() {
			statuses, err := observer.ObservePlugins(t.Context(), request.Generation, request.Digests)
			done <- result{statuses: statuses, err: err}
		}()
	}
	for range 8 {
		await.Rcv(t, started)
	}
	unblock()
	for range 2 {
		outcome := await.Rcv(t, done)
		require.NoError(t, outcome.err)
		require.Len(t, outcome.statuses[request.Digests[0]], 9)
	}
	mu.Lock()
	gotMaximum, gotActive, gotCompleted := maximum, active, completed
	mu.Unlock()
	require.Equal(t, 8, gotMaximum)
	require.Zero(t, gotActive)
	require.Equal(t, 18, gotCompleted)
}

type pluginTestHost struct{ id, address string }

func (h pluginTestHost) Identity() string   { return h.id }
func (h pluginTestHost) GetAddress() string { return h.address }

func TestPluginMembershipBounds(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		hosts []membership.HostInfo
		code  codes.Code
	}{
		{name: "no members", hosts: []membership.HostInfo{}},
		{name: "missing member", hosts: []membership.HostInfo{nil}, code: codes.DataLoss},
		{name: "missing identity", hosts: []membership.HostInfo{pluginTestHost{address: "a:7239"}}, code: codes.DataLoss},
		{name: "missing address", hosts: []membership.HostInfo{pluginTestHost{id: "a"}}, code: codes.DataLoss},
		{name: "duplicate identity", hosts: []membership.HostInfo{pluginTestHost{"a", "a:7239"}, pluginTestHost{"a", "b:7239"}}, code: codes.DataLoss},
		{name: "duplicate address", hosts: []membership.HostInfo{pluginTestHost{"a", "a:7239"}, pluginTestHost{"b", "a:7239"}}, code: codes.DataLoss},
		{name: "oversized identity", hosts: []membership.HostInfo{pluginTestHost{strings.Repeat("x", 513), "a:7239"}}, code: codes.DataLoss},
		{name: "invalid address", hosts: []membership.HostInfo{pluginTestHost{"a", "a\nb"}}, code: codes.DataLoss},
		{name: "too many hosts", hosts: make([]membership.HostInfo, MaxPluginHosts+1), code: codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			members, err := pluginMembers(test.hosts)
			require.Equal(t, test.code, serviceerror.ToStatus(err).Code())
			require.Empty(t, members)
		})
	}
	t.Run("full detached sorted cut", func(t *testing.T) {
		t.Parallel()
		hosts := make([]membership.HostInfo, MaxPluginHosts)
		expected := make([]pluginMember, MaxPluginHosts)
		for i := range hosts {
			address := fmt.Sprintf("member-%02d:7239", i)
			hosts[len(hosts)-i-1] = membership.NewHostInfoFromAddress(address)
			expected[i] = pluginMember{id: address, address: address}
		}
		members, err := pluginMembers(hosts)
		require.NoError(t, err)
		require.Equal(t, expected, members)
	})
}

func TestPluginObserverRequestBoundsBeforeMembership(t *testing.T) {
	t.Parallel()
	request, _ := pluginObservationFixture(time.Now(), 1, 1)
	for _, test := range []struct {
		name       string
		generation uint64
		digests    []string
		code       codes.Code
	}{
		{name: "empty page"},
		{name: "no intent cut", digests: request.Digests, code: codes.InvalidArgument},
		{name: "mutable pin", generation: 1, digests: []string{"latest"}, code: codes.InvalidArgument},
		{name: "duplicate pin", generation: 1, digests: []string{request.Digests[0], request.Digests[0]}, code: codes.InvalidArgument},
		{name: "too many pins", generation: 1, digests: make([]string, 201), code: codes.InvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			statuses, err := NewObserver(nil, nil, nil).ObservePlugins(t.Context(), test.generation, test.digests)
			require.Equal(t, test.code, serviceerror.ToStatus(err).Code())
			require.Nil(t, statuses)
		})
	}

}

func TestPluginMemberEvidenceExpiresWhileOtherRPCsComplete(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	request, response := pluginObservationFixture(now.Add(-119*time.Second), 1, 1)
	statuses, err := pluginObservationStatuses(request, response, "member:7239", now)
	require.NoError(t, err)
	observations := []pluginMemberObservation{{
		storageHostID: response.StorageHostId, processBootID: response.ProcessBootId, statuses: statuses,
	}}
	fencePluginMemberObservations(observations, now.Add(2*time.Second))
	require.NotEmpty(t, observations[0].problem)
}
