//go:build integration

package tests

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchregistry"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type clusterPluginLostReplyClient struct {
	registrypb.ClusterPluginServiceClient
	dropped bool
}

func (c *clusterPluginLostReplyClient) InstallClusterPlugin(
	ctx context.Context,
	request *registrypb.InstallClusterPluginRequest,
	options ...grpc.CallOption,
) (*registrypb.InstallClusterPluginResponse, error) {
	response, err := c.ClusterPluginServiceClient.InstallClusterPlugin(ctx, request, options...)
	if err == nil && !c.dropped {
		c.dropped = true
		return nil, serviceerror.NewUnavailable("plugin intent response lost after History commit")
	}
	return response, err
}

func (c *clusterPluginLostReplyClient) LoadClusterPlugin(
	ctx context.Context,
	request *registrypb.LoadClusterPluginRequest,
	options ...grpc.CallOption,
) (*registrypb.LoadClusterPluginResponse, error) {
	response, err := c.ClusterPluginServiceClient.LoadClusterPlugin(ctx, request, options...)
	if err == nil && !c.dropped {
		c.dropped = true
		return nil, serviceerror.NewUnavailable("plugin load response lost after History commit")
	}
	return response, err
}

func TestDrasiClusterPluginIntentRPC(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
	ctx := testcontext.For(t)
	tv := testvars.New(t)
	client := env.ClusterPluginClient()
	read := &registrypb.ReadClusterPluginsRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: watchregistry.ClusterPluginBusinessID,
		Operation: &registrypb.ReadClusterPluginsRequest_Snapshot{Snapshot: true},
	}
	before, err := client.ReadClusterPlugins(ctx, read)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(tv.RequestID()))
	artifact := &contractpb.Plugin{
		CatalogId: "trusted", Id: "fixture-" + tv.RequestID(), Version: "1.0.0",
		Digest: fmt.Sprintf("sha256:%x", digest), Compatible: true, Message: "host-specific compatibility",
		Descriptors: []*contractpb.PluginDescriptor{{
			Component: "source", Category: contractpb.PluginCategory_PLUGIN_CATEGORY_SOURCE, Supported: true,
		}},
	}
	request := &registrypb.InstallClusterPluginRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: watchregistry.ClusterPluginBusinessID,
		Request: &contractpb.InstallPluginRequest{
			CatalogId: artifact.CatalogId, PluginId: artifact.Id, Version: artifact.Version,
			Digest: artifact.Digest, RequestId: tv.RequestID(),
		},
		Resolution: &registrypb.InstallClusterPluginRequest_Plugin{Plugin: artifact},
	}
	expected := common.CloneProto(artifact)
	expected.Compatible, expected.Message, expected.Descriptors[0].Supported = false, "", false
	type result struct {
		response *registrypb.InstallClusterPluginResponse
		err      error
	}
	completed := make(chan result, 8)
	start := make(chan struct{})
	for range cap(completed) {
		go func() {
			<-start
			response, err := client.InstallClusterPlugin(ctx, request)
			completed <- result{response, err}
		}()
	}
	close(start)
	for range cap(completed) {
		actual := await.Rcv(t, completed)
		require.NoError(t, actual.err)
		protorequire.ProtoEqual(t, expected, actual.response.GetPlugin())
	}
	after, err := client.ReadClusterPlugins(ctx, read)
	require.NoError(t, err)
	require.Equal(t, before.Generation+1, after.Generation)
	require.Len(t, after.Plugins, len(before.Plugins)+1)

	lost := &clusterPluginLostReplyClient{ClusterPluginServiceClient: client}
	retry := common.CloneProto(request)
	retry.Request.RequestId += "-lost"
	_, err = lost.InstallClusterPlugin(ctx, retry)
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	retry.Resolution = &registrypb.InstallClusterPluginRequest_Rejection{Rejection: &registrypb.Rejection{
		Code: registrypb.REJECTION_CODE_NOT_FOUND, Message: "configured release was removed",
	}}
	response, err := lost.InstallClusterPlugin(ctx, retry)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, response.GetPlugin())
	retry.Request.Version = "2.0.0"
	_, err = client.InstallClusterPlugin(ctx, retry)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	retry.Request = common.CloneProto(request.Request)
	retry.NamespaceId = env.NamespaceID().String()
	_, err = client.InstallClusterPlugin(ctx, retry)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	final, err := client.ReadClusterPlugins(ctx, read)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, after, final)

	second := common.CloneProto(request)
	second.Request.RequestId += "-second"
	second.Request.Version, second.GetPlugin().Version = "2.0.0", "2.0.0"
	secondDigest := sha256.Sum256([]byte(second.Request.RequestId))
	second.Request.Digest = fmt.Sprintf("sha256:%x", secondDigest)
	second.GetPlugin().Digest = second.Request.Digest
	_, err = client.InstallClusterPlugin(ctx, second)
	require.NoError(t, err)
	type loadResult struct {
		digest   string
		response *registrypb.LoadClusterPluginResponse
		err      error
	}
	loads := make(chan loadResult, 2)
	loadStart := make(chan struct{})
	commands := []*registrypb.LoadClusterPluginRequest{
		{
			NamespaceId: request.NamespaceId, ExecutionId: request.ExecutionId,
			Request: &contractpb.LoadPluginRequest{Digest: request.Request.Digest, RequestId: tv.RequestID() + "-load-first"},
		},
		{
			NamespaceId: request.NamespaceId, ExecutionId: request.ExecutionId,
			Request: &contractpb.LoadPluginRequest{Digest: second.Request.Digest, RequestId: tv.RequestID() + "-load-second"},
		},
	}
	for _, command := range commands {
		go func() {
			<-loadStart
			response, err := client.LoadClusterPlugin(ctx, command)
			loads <- loadResult{command.Request.Digest, response, err}
		}()
	}
	close(loadStart)
	for range cap(loads) {
		actual := await.Rcv(t, loads)
		require.NoError(t, actual.err)
		require.Equal(t, actual.digest, actual.response.GetDigest())
	}
	selected, err := client.ReadClusterPlugins(ctx, read)
	require.NoError(t, err)
	require.Equal(t, final.Generation+3, selected.Generation)
	require.Contains(t, []string{request.Request.Digest, second.Request.Digest}, selected.DesiredLoads[artifact.Id])
	for _, command := range commands {
		_, err := client.LoadClusterPlugin(ctx, command)
		require.NoError(t, err)
	}
	replayed, err := client.ReadClusterPlugins(ctx, read)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, selected, replayed)
	desired := common.CloneProto(commands[0])
	if desired.Request.Digest == selected.DesiredLoads[artifact.Id] {
		desired = common.CloneProto(commands[1])
	}
	desired.Request.RequestId += "-lost"
	lostLoad := &clusterPluginLostReplyClient{ClusterPluginServiceClient: client}
	_, err = lostLoad.LoadClusterPlugin(ctx, desired)
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	committed, err := client.ReadClusterPlugins(ctx, read)
	require.NoError(t, err)
	require.Equal(t, selected.Generation+1, committed.Generation)
	require.Equal(t, desired.Request.Digest, committed.DesiredLoads[artifact.Id])
	desired.Rejection = &registrypb.Rejection{
		Code: registrypb.REJECTION_CODE_FAILED_PRECONDITION, Message: "acceptance is disabled",
	}
	_, err = lostLoad.LoadClusterPlugin(ctx, desired)
	require.NoError(t, err)
	replayed, err = client.ReadClusterPlugins(ctx, read)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, committed, replayed)
}
