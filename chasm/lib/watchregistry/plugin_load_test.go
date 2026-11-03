package watchregistry

import (
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func clusterPluginLoadRequest(digest, requestID string) *registrypb.LoadClusterPluginRequest {
	return &registrypb.LoadClusterPluginRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID,
		Request: &contractpb.LoadPluginRequest{Digest: digest, RequestId: requestID},
	}
}

func TestClusterPluginLoadSupersessionSurvivesSnapshot(t *testing.T) {
	t.Parallel()
	first := clusterPluginRequest()
	second := clusterPluginRequest()
	second.Request.RequestId, second.Request.Version = "install-second", "2.0.0"
	second.Request.Digest = "sha256:" + strings.Repeat("b", 64)
	second.GetPlugin().Version, second.GetPlugin().Digest = second.Request.Version, second.Request.Digest
	firstLoad := clusterPluginLoadRequest(first.Request.Digest, "load-first")
	var env *clusterPluginEnvironment
	env = newClusterPluginEnvironment(t, defaultClusterPluginLimits(),
		chasmtest.WithInvariantCheck(func(t *testing.T, node *chasm.Node, root chasm.RootComponent) {
			restored := restoreClusterPluginSnapshot(t, node, env.registry)
			protorequire.ProtoEqual(t, root.(*clusterPlugins).ClusterPluginState, restored.ClusterPluginState)
			if _, ok := restored.Receipts[firstLoad.Request.RequestId]; ok {
				before := common.CloneProto(restored.ClusterPluginState)
				response, err := restored.load(nil, firstLoad)
				require.NoError(t, err)
				require.Equal(t, first.Request.Digest, response.GetDigest())
				protorequire.ProtoEqual(t, before, restored.ClusterPluginState)
			}
		}),
	)
	for _, request := range []*registrypb.InstallClusterPluginRequest{first, second} {
		_, err := env.handler.InstallClusterPlugin(env.ctx, request)
		require.NoError(t, err)
	}
	for _, request := range []*registrypb.LoadClusterPluginRequest{
		firstLoad, clusterPluginLoadRequest(second.Request.Digest, "load-second"),
	} {
		response, err := env.handler.LoadClusterPlugin(env.ctx, request)
		require.NoError(t, err)
		require.Equal(t, request.Request.Digest, response.GetDigest())
	}
	expected := env.state(t)
	require.Equal(t, uint64(4), expected.Generation)
	require.Equal(t, map[string]string{first.Request.PluginId: second.Request.Digest}, expected.DesiredLoads)
	retry := common.CloneProto(firstLoad)
	retry.Rejection = &registrypb.Rejection{
		Code: registrypb.REJECTION_CODE_FAILED_PRECONDITION, Message: "plugin acceptance is disabled",
	}
	response, err := env.handler.LoadClusterPlugin(env.ctx, retry)
	require.NoError(t, err)
	require.Equal(t, first.Request.Digest, response.GetDigest())
	protorequire.ProtoEqual(t, expected, env.state(t))
	retry.Request.Digest = second.Request.Digest
	_, err = env.handler.LoadClusterPlugin(env.ctx, retry)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, expected, env.state(t))
	view := env.snapshot(t)
	view.DesiredLoads[first.Request.PluginId] = first.Request.Digest
	protorequire.ProtoEqual(t, expected, env.state(t))
	retry.Request.RequestId, retry.Request.Digest, retry.Rejection = "load-second-again", second.Request.Digest, nil
	_, err = env.handler.LoadClusterPlugin(env.ctx, retry)
	require.NoError(t, err)
	require.Equal(t, expected.Generation, env.state(t).Generation)
}

func TestClusterPluginLoadRejectionIsPermanent(t *testing.T) {
	t.Parallel()
	env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
	install := clusterPluginRequest()
	_, err := env.handler.InstallClusterPlugin(env.ctx, install)
	require.NoError(t, err)
	load := clusterPluginLoadRequest(install.Request.Digest, "load")
	load.Rejection = &registrypb.Rejection{
		Code: registrypb.REJECTION_CODE_FAILED_PRECONDITION, Message: "plugin acceptance is disabled",
	}
	_, err = env.handler.LoadClusterPlugin(env.ctx, load)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	before := env.state(t)
	load.Rejection = nil
	_, err = env.handler.LoadClusterPlugin(env.ctx, load)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.state(t))
	require.Empty(t, env.snapshot(t).DesiredLoads)
}

func TestClusterPluginLoadIdentityIsSharedAcrossCatalogs(t *testing.T) {
	t.Parallel()
	env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
	first := clusterPluginRequest()
	second := clusterPluginRequest()
	second.Request.RequestId, second.Request.CatalogId = "install-second", "another-catalog"
	second.Request.Digest = "sha256:" + strings.Repeat("b", 64)
	second.GetPlugin().CatalogId, second.GetPlugin().Digest = second.Request.CatalogId, second.Request.Digest
	for _, request := range []*registrypb.InstallClusterPluginRequest{first, second} {
		_, err := env.handler.InstallClusterPlugin(env.ctx, request)
		require.NoError(t, err)
		_, err = env.handler.LoadClusterPlugin(env.ctx, clusterPluginLoadRequest(request.Request.Digest, request.Request.RequestId+"-load"))
		require.NoError(t, err)
	}
	require.Equal(t, map[string]string{first.Request.PluginId: second.Request.Digest}, env.state(t).DesiredLoads)
}

func TestClusterPluginLoadMissingInstallationIsPermanent(t *testing.T) {
	t.Parallel()
	env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
	install := clusterPluginRequest()
	load := clusterPluginLoadRequest(install.Request.Digest, "load-before-install")
	_, err := env.handler.LoadClusterPlugin(env.ctx, load)
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	_, err = env.handler.InstallClusterPlugin(env.ctx, install)
	require.NoError(t, err)
	before := env.state(t)
	_, err = env.handler.LoadClusterPlugin(env.ctx, load)
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.state(t))
	load.Request.RequestId = "load-after-install"
	_, err = env.handler.LoadClusterPlugin(env.ctx, load)
	require.NoError(t, err)
	require.Equal(t, map[string]string{install.Request.PluginId: install.Request.Digest}, env.state(t).DesiredLoads)
}

func TestClusterPluginMethodsShareRequestIdentities(t *testing.T) {
	t.Parallel()
	for _, installFirst := range []bool{false, true} {
		name := "load then install"
		if installFirst {
			name = "install then load"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
			install := clusterPluginRequest()
			load := clusterPluginLoadRequest(install.Request.Digest, install.Request.RequestId)
			if installFirst {
				_, err := env.handler.InstallClusterPlugin(env.ctx, install)
				require.NoError(t, err)
				before := env.state(t)
				_, err = env.handler.LoadClusterPlugin(env.ctx, load)
				require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
				protorequire.ProtoEqual(t, before, env.state(t))
			} else {
				_, err := env.handler.LoadClusterPlugin(env.ctx, load)
				require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
				before := env.state(t)
				_, err = env.handler.InstallClusterPlugin(env.ctx, install)
				require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
				protorequire.ProtoEqual(t, before, env.state(t))
			}
		})
	}
}

func TestClusterPluginLoadCapacityRefusalIsAtomic(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"permanent requests", "complete retained state"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			limits := defaultClusterPluginLimits()
			install := clusterPluginRequest()
			if name == "permanent requests" {
				limits.Requests = 1
			} else {
				limits.StateBytes = 4096
				probe, err := newClusterPlugins(limits)
				require.NoError(t, err)
				_, err = probe.install(nil, install)
				require.NoError(t, err)
				install.GetPlugin().Provenance = strings.Repeat("p", int(limits.StateBytes)-proto.Size(probe.ClusterPluginState)-16)
			}
			env := newClusterPluginEnvironment(t, limits)
			accepted, err := env.handler.InstallClusterPlugin(env.ctx, install)
			require.NoError(t, err)
			before := env.state(t)
			_, err = env.handler.LoadClusterPlugin(env.ctx, clusterPluginLoadRequest(install.Request.Digest, "load"))
			require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, before, env.state(t))
			retry, err := env.handler.InstallClusterPlugin(env.ctx, install)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, accepted, retry)
		})
	}
}

func TestClusterPluginLoadBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*registrypb.LoadClusterPluginRequest)
	}{
		{"foreign namespace", func(r *registrypb.LoadClusterPluginRequest) {
			r.NamespaceId = "00000000-0000-4000-8000-000000000001"
		}},
		{"namespace registry key", func(r *registrypb.LoadClusterPluginRequest) { r.ExecutionId = BusinessID }},
		{"missing public request", func(r *registrypb.LoadClusterPluginRequest) { r.Request = nil }},
		{"invalid digest", func(r *registrypb.LoadClusterPluginRequest) { r.Request.Digest = "tag" }},
		{"transient rejection", func(r *registrypb.LoadClusterPluginRequest) {
			r.Rejection = &registrypb.Rejection{Code: registrypb.RejectionCode(codes.Unavailable), Message: "offline"}
		}},
		{"empty rejection", func(r *registrypb.LoadClusterPluginRequest) {
			r.Rejection = &registrypb.Rejection{Code: registrypb.REJECTION_CODE_FAILED_PRECONDITION}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
			request := clusterPluginLoadRequest(clusterPluginRequest().Request.Digest, "load")
			tc.change(request)
			_, err := env.handler.LoadClusterPlugin(env.ctx, request)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{}, env.snapshot(t))
		})
	}
}
