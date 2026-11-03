package watch

import (
	"context"
	"fmt"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchregistry"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc"
)

func pluginSnapshot() *registrypb.ReadClusterPluginsResponse {
	plugin := &contractpb.Plugin{
		CatalogId: "trusted", Id: "fixture-source", Version: "1.0.0",
		Digest: "sha256:" + strings.Repeat("a", 64),
	}
	return &registrypb.ReadClusterPluginsResponse{
		Generation: 2, Plugins: []*contractpb.Plugin{plugin},
		DesiredLoads: map[string]string{plugin.Id: plugin.Digest},
	}
}

type pluginIntentClient struct {
	registrypb.ClusterPluginServiceClient
	read func(context.Context, *registrypb.ReadClusterPluginsRequest) (*registrypb.ReadClusterPluginsResponse, error)
}

func (c *pluginIntentClient) ReadClusterPlugins(ctx context.Context, request *registrypb.ReadClusterPluginsRequest, _ ...grpc.CallOption) (*registrypb.ReadClusterPluginsResponse, error) {
	return c.read(ctx, request)
}

func TestReadPluginIntent(t *testing.T) {
	t.Parallel()
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprintf("unavailable=%t", unavailable), func(t *testing.T) {
			t.Parallel()
			snapshot := pluginSnapshot()
			failure := serviceerror.NewUnavailable("History unavailable")
			client := &pluginIntentClient{read: func(_ context.Context, request *registrypb.ReadClusterPluginsRequest) (*registrypb.ReadClusterPluginsResponse, error) {
				protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsRequest{
					NamespaceId: primitives.SystemNamespaceID, ExecutionId: watchregistry.ClusterPluginBusinessID,
					Operation: &registrypb.ReadClusterPluginsRequest_Snapshot{Snapshot: true},
				}, request)
				if unavailable {
					return nil, failure
				}
				return snapshot, nil
			}}
			view, err := readPluginIntent(t.Context(), client)
			if unavailable {
				require.Nil(t, view)
				require.ErrorIs(t, err, failure)
				return
			}
			require.NoError(t, err)
			protorequire.ProtoEqual(t, snapshot, view.snapshot)
			require.NotSame(t, snapshot, view.snapshot)
			snapshot.Plugins[0].Version = "changed"
			clear(snapshot.DesiredLoads)
			require.Equal(t, "1.0.0", view.byDigest[view.snapshot.Plugins[0].Digest].Version)
			require.Len(t, view.snapshot.DesiredLoads, 1)
		})
	}
}

func TestParsePluginIntent(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*registrypb.ReadClusterPluginsResponse)
	}{
		{"receipt instead of snapshot", func(s *registrypb.ReadClusterPluginsResponse) { s.Receipt = &registrypb.ClusterPluginReceipt{} }},
		{"zero generation with state", func(s *registrypb.ReadClusterPluginsResponse) { s.Generation = 0 }},
		{"nonzero generation without installations", func(s *registrypb.ReadClusterPluginsResponse) { s.Plugins, s.DesiredLoads = nil, nil }},
		{"missing metadata", func(s *registrypb.ReadClusterPluginsResponse) { s.Plugins[0] = nil }},
		{"malformed digest", func(s *registrypb.ReadClusterPluginsResponse) { s.Plugins[0].Digest = "latest" }},
		{"invalid selection", func(s *registrypb.ReadClusterPluginsResponse) { s.Plugins[0].CatalogId = "../catalog" }},
		{"host compatibility", func(s *registrypb.ReadClusterPluginsResponse) { s.Plugins[0].Compatible = true }},
		{"host diagnostic", func(s *registrypb.ReadClusterPluginsResponse) { s.Plugins[0].Message = "loaded" }},
		{"missing descriptor", func(s *registrypb.ReadClusterPluginsResponse) {
			s.Plugins[0].Descriptors = []*contractpb.PluginDescriptor{nil}
		}},
		{"host-supported descriptor", func(s *registrypb.ReadClusterPluginsResponse) {
			s.Plugins[0].Descriptors = []*contractpb.PluginDescriptor{{Supported: true}}
		}},
		{"duplicate installation", func(s *registrypb.ReadClusterPluginsResponse) {
			s.Plugins = append(s.Plugins, common.CloneProto(s.Plugins[0]))
		}},
		{"unordered installations", func(s *registrypb.ReadClusterPluginsResponse) {
			next := common.CloneProto(s.Plugins[0])
			next.Digest = "sha256:" + strings.Repeat("b", 64)
			s.Plugins = append([]*contractpb.Plugin{next}, s.Plugins...)
		}},
		{"unknown desired digest", func(s *registrypb.ReadClusterPluginsResponse) {
			s.DesiredLoads[s.Plugins[0].Id] = "sha256:" + strings.Repeat("b", 64)
		}},
		{"different native identity", func(s *registrypb.ReadClusterPluginsResponse) {
			s.DesiredLoads = map[string]string{"other": s.Plugins[0].Digest}
		}},
		{"oversized metadata", func(s *registrypb.ReadClusterPluginsResponse) {
			s.Plugins[0].Provenance = strings.Repeat("x", watchregistry.MaxClusterPluginMetadataBytes)
		}},
		{"oversized complete cut", func(s *registrypb.ReadClusterPluginsResponse) {
			s.Plugins[0].Provenance = strings.Repeat("x", watchregistry.MaxStateBytes)
		}},
		{"too many installations", func(s *registrypb.ReadClusterPluginsResponse) {
			for len(s.Plugins) <= watchregistry.MaxClusterPlugins {
				s.Plugins = append(s.Plugins, common.CloneProto(s.Plugins[0]))
			}
			s.Generation = uint64(len(s.Plugins))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			snapshot := pluginSnapshot()
			test.change(snapshot)
			view, err := parsePluginIntent(snapshot)
			require.Nil(t, view)
			var dataLoss *serviceerror.DataLoss
			require.ErrorAs(t, err, &dataLoss)
		})
	}
	t.Run("nil is not an empty root", func(t *testing.T) {
		t.Parallel()
		view, err := parsePluginIntent(nil)
		require.Nil(t, view)
		require.Error(t, err)
	})
	t.Run("explicit empty root", func(t *testing.T) {
		t.Parallel()
		view, err := parsePluginIntent(&registrypb.ReadClusterPluginsResponse{})
		require.NoError(t, err)
		require.Empty(t, view.byDigest)
		require.Zero(t, view.snapshot.Generation)
	})
	t.Run("maximum installations", func(t *testing.T) {
		t.Parallel()
		snapshot := &registrypb.ReadClusterPluginsResponse{Generation: watchregistry.MaxClusterPlugins}
		for i := 1; i <= watchregistry.MaxClusterPlugins; i++ {
			snapshot.Plugins = append(snapshot.Plugins, &contractpb.Plugin{
				CatalogId: "trusted", Id: fmt.Sprintf("fixture-%03d", i), Version: "1.0.0",
				Digest: fmt.Sprintf("sha256:%064x", i),
			})
		}
		view, err := parsePluginIntent(snapshot)
		require.NoError(t, err)
		require.Len(t, view.byDigest, watchregistry.MaxClusterPlugins)
	})
}

func TestPluginIntentFollows(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*registrypb.ReadClusterPluginsResponse)
		valid  bool
	}{
		{name: "identical cut", valid: true},
		{name: "new desired cut", valid: true, change: func(s *registrypb.ReadClusterPluginsResponse) {
			next := common.CloneProto(s.Plugins[0])
			next.Version, next.Digest = "2.0.0", "sha256:"+strings.Repeat("b", 64)
			s.Generation += 2
			s.Plugins = append(s.Plugins, next)
			s.DesiredLoads[next.Id] = next.Digest
		}},
		{name: "older generation", change: func(s *registrypb.ReadClusterPluginsResponse) { s.Generation-- }},
		{name: "same generation different selection", change: func(s *registrypb.ReadClusterPluginsResponse) { clear(s.DesiredLoads) }},
		{name: "removed desired load", change: func(s *registrypb.ReadClusterPluginsResponse) { s.Generation++; clear(s.DesiredLoads) }},
		{name: "changed immutable metadata", change: func(s *registrypb.ReadClusterPluginsResponse) { s.Generation++; s.Plugins[0].Version = "2.0.0" }},
		{name: "removed immutable installation", change: func(s *registrypb.ReadClusterPluginsResponse) {
			s.Generation++
			s.Plugins[0].Digest = "sha256:" + strings.Repeat("b", 64)
			s.DesiredLoads[s.Plugins[0].Id] = s.Plugins[0].Digest
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			previous, err := parsePluginIntent(pluginSnapshot())
			require.NoError(t, err)
			snapshot := pluginSnapshot()
			if test.change != nil {
				test.change(snapshot)
			}
			next, err := parsePluginIntent(snapshot)
			require.NoError(t, err)
			err = next.follows(previous)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
