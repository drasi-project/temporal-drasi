package watch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/watchservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
)

func TestPluginObserverCancellationJoinsDispatchers(t *testing.T) {
	t.Parallel()
	started, stopped := make(chan struct{}, 8), make(chan struct{}, 8)
	conn, stop := pluginObservationRPC(t, &pluginObservationTestServer{
		observe: func(ctx context.Context, _ *watchservice.ObservePluginsRequest) (*watchservice.ObservePluginsResponse, error) {
			started <- struct{}{}
			<-ctx.Done()
			stopped <- struct{}{}
			return nil, ctx.Err()
		},
	})
	defer stop()
	members := make([]membership.HostInfo, 9)
	for i := range members {
		members[i] = membership.NewHostInfoFromAddress(fmt.Sprintf("member-%02d:7239", i))
	}
	controller := gomock.NewController(t)
	monitor, resolver := membership.NewMockMonitor(controller), membership.NewMockServiceResolver(controller)
	monitor.EXPECT().GetResolver(primitives.WatchService).Return(resolver, nil)
	resolver.EXPECT().Members().Return(members)
	factory := common.NewMockRPCFactory(controller)
	factory.EXPECT().CreateWatchGRPCConnection(gomock.Any()).Return(conn).Times(8)
	observer := NewObserver(nil, monitor, factory)
	request, _ := pluginObservationFixture(time.Now(), 1, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := observer.ObservePlugins(ctx, request.Generation, request.Digests)
		done <- err
	}()
	for range 8 {
		await.Rcv(t, started)
	}
	cancel()
	require.ErrorIs(t, await.Rcv(t, done), context.Canceled)
	for range 8 {
		await.Rcv(t, stopped)
	}
	require.Empty(t, observer.pluginSlots)
}

func TestChangedPluginMembershipBounds(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		previous []pluginMember
		current  []pluginMember
		want     []pluginMember
	}{
		{name: "removed member remains visible",
			previous: []pluginMember{{id: "a", address: "a:7239"}}, want: []pluginMember{{id: "a", address: "a:7239"}}},
		{name: "replaced address keeps current address",
			previous: []pluginMember{{id: "a", address: "old:7239"}}, current: []pluginMember{{id: "a", address: "new:7239"}},
			want: []pluginMember{{id: "a", address: "new:7239"}}},
		{name: "added member is sorted",
			previous: []pluginMember{{id: "b", address: "b:7239"}}, current: []pluginMember{{id: "a", address: "a:7239"}},
			want: []pluginMember{{id: "a", address: "a:7239"}, {id: "b", address: "b:7239"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			members, err := changedPluginMembers(test.previous, test.current)
			require.NoError(t, err)
			require.Equal(t, test.want, members)
		})
	}
	t.Run("oversized union is not truncated", func(t *testing.T) {
		t.Parallel()
		previous, current := make([]pluginMember, MaxPluginHosts), make([]pluginMember, MaxPluginHosts)
		for i := range MaxPluginHosts {
			previous[i] = pluginMember{id: fmt.Sprintf("old-%02d", i)}
			current[i] = pluginMember{id: fmt.Sprintf("new-%02d", i)}
		}
		members, err := changedPluginMembers(previous, current)
		require.Nil(t, members)
		require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	})
}
