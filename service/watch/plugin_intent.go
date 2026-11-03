package watch

import (
	"context"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchregistry"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/proto"
)

// pluginIntent is an immutable complete cut shared by local runtime workers.
type pluginIntent struct {
	snapshot *registrypb.ReadClusterPluginsResponse
	byDigest map[string]*contractpb.Plugin
}

func readPluginIntent(ctx context.Context, client registrypb.ClusterPluginServiceClient) (*pluginIntent, error) {
	response, err := client.ReadClusterPlugins(ctx, &registrypb.ReadClusterPluginsRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: watchregistry.ClusterPluginBusinessID,
		Operation: &registrypb.ReadClusterPluginsRequest_Snapshot{Snapshot: true},
	})
	if err != nil {
		return nil, err
	}
	return parsePluginIntent(response)
}

func parsePluginIntent(response *registrypb.ReadClusterPluginsResponse) (*pluginIntent, error) {
	if err := watchregistry.ValidateClusterPluginSnapshot(response); err != nil {
		return nil, err
	}
	view := &pluginIntent{
		snapshot: common.CloneProto(response),
		byDigest: make(map[string]*contractpb.Plugin, len(response.Plugins)),
	}
	for _, plugin := range view.snapshot.Plugins {
		view.byDigest[plugin.Digest] = plugin
	}
	return view, nil
}

func (v *pluginIntent) follows(previous *pluginIntent) error {
	if previous != nil && (v.snapshot.Generation < previous.snapshot.Generation ||
		(v.snapshot.Generation == previous.snapshot.Generation && !proto.Equal(v.snapshot, previous.snapshot))) {
		return serviceerror.NewFailedPrecondition("cluster plugin intent regressed or changed under the same generation")
	}
	if previous != nil {
		for digest, plugin := range previous.byDigest {
			if !proto.Equal(plugin, v.byDigest[digest]) {
				return serviceerror.NewDataLoss("cluster plugin intent removed or changed a retained immutable installation")
			}
		}
		for id := range previous.snapshot.DesiredLoads {
			if _, ok := v.snapshot.DesiredLoads[id]; !ok {
				return serviceerror.NewDataLoss("cluster plugin intent removed a retained desired load")
			}
		}
	}
	return nil
}
