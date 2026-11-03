package watchregistry

import (
	"context"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

type clusterPluginEnvironment struct {
	ctx      context.Context
	handler  *clusterPluginHandler
	ref      chasm.ComponentRef
	registry *chasm.Registry
}

func newClusterPluginEnvironment(t *testing.T, limits *registrypb.ClusterPluginLimits, options ...chasmtest.EngineOption) *clusterPluginEnvironment {
	t.Helper()
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
		Id: primitives.SystemNamespaceID, Name: primitives.SystemLocalNamespace,
	}, nil, "active")
	namespaces := namespace.NewMockRegistry(gomock.NewController(t))
	namespaces.EXPECT().GetNamespaceByID(namespace.ID(primitives.SystemNamespaceID)).Return(ns, nil).AnyTimes()
	registry := chasm.NewRegistry(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly))
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(&Library{}))
	engine := chasmtest.NewEngine(t, registry, options...)
	return &clusterPluginEnvironment{
		ctx:      chasm.NewEngineContext(testcontext.For(t), engine),
		handler:  &clusterPluginHandler{namespaces: namespaces, limits: limits},
		registry: registry,
		ref: chasm.NewComponentRef[*clusterPlugins](chasm.ExecutionKey{
			NamespaceID: primitives.SystemNamespaceID, BusinessID: ClusterPluginBusinessID,
		}),
	}
}

func TestClusterPluginSnapshotPreservesRetryIdentity(t *testing.T) {
	t.Parallel()
	request := clusterPluginRequest()
	var env *clusterPluginEnvironment
	env = newClusterPluginEnvironment(t, defaultClusterPluginLimits(),
		chasmtest.WithInvariantCheck(func(t *testing.T, node *chasm.Node, root chasm.RootComponent) {
			restored := restoreClusterPluginSnapshot(t, node, env.registry)
			protorequire.ProtoEqual(t, root.(*clusterPlugins).ClusterPluginState, restored.ClusterPluginState)
			require.Equal(t, chasm.LifecycleStateRunning, restored.LifecycleState(nil))
			retry := common.CloneProto(request)
			retry.Resolution = nil
			response, err := restored.install(nil, retry)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, root.(*clusterPlugins).Installations[request.Request.Digest], response.GetPlugin())
		}),
	)
	_, err := env.handler.InstallClusterPlugin(env.ctx, request)
	require.NoError(t, err)
	_, _, err = chasm.UpdateComponent(env.ctx, env.ref,
		func(c *clusterPlugins, ctx chasm.MutableContext, input chasm.NoValue) (chasm.NoValue, error) {
			_, err := c.Terminate(ctx, chasm.TerminateComponentRequest{})
			return input, err
		}, nil)
	require.ErrorContains(t, err, "cannot be terminated")
}

func restoreClusterPluginSnapshot(t *testing.T, node *chasm.Node, registry *chasm.Registry) *clusterPlugins {
	t.Helper()
	reloaded, err := chasm.NewTreeFromDB(node.Snapshot(nil).Nodes, registry, &chasm.MockNodeBackend{},
		chasm.DefaultPathEncoder, testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	persisted, err := reloaded.Component(chasm.NewContext(testcontext.For(t), reloaded), chasm.ComponentRef{})
	require.NoError(t, err)
	return persisted.(*clusterPlugins)
}

func clusterPluginRequest() *registrypb.InstallClusterPluginRequest {
	request := &contractpb.InstallPluginRequest{
		CatalogId: "trusted", PluginId: "plugin", Version: "1.0.0",
		Digest: "sha256:" + strings.Repeat("a", 64), RequestId: "install",
	}
	return &registrypb.InstallClusterPluginRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID, Request: request,
		Resolution: &registrypb.InstallClusterPluginRequest_Plugin{Plugin: &contractpb.Plugin{
			CatalogId: request.CatalogId, Id: request.PluginId, Version: request.Version, Digest: request.Digest,
			SignerIdentity: "advertised-not-verified", Compatible: true, Message: "current host supports this release",
			Descriptors: []*contractpb.PluginDescriptor{{
				Component: "source", Category: contractpb.PluginCategory_PLUGIN_CATEGORY_SOURCE, Supported: true,
				ConfigurationSchema: &structpb.Struct{Fields: map[string]*structpb.Value{
					"type": structpb.NewStringValue("object"),
				}},
			}},
		}},
	}
}

func (e *clusterPluginEnvironment) snapshot(t *testing.T) *registrypb.ReadClusterPluginsResponse {
	t.Helper()
	value, err := e.handler.ReadClusterPlugins(e.ctx, &registrypb.ReadClusterPluginsRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID,
		Operation: &registrypb.ReadClusterPluginsRequest_Snapshot{Snapshot: true},
	})
	require.NoError(t, err)
	return value
}

func (e *clusterPluginEnvironment) state(t *testing.T) *registrypb.ClusterPluginState {
	t.Helper()
	value, err := chasm.ReadComponent(e.ctx, e.ref,
		func(c *clusterPlugins, _ chasm.Context, _ chasm.NoValue) (*registrypb.ClusterPluginState, error) {
			return common.CloneProto(c.ClusterPluginState), nil
		}, nil)
	require.NoError(t, err)
	return value
}

func TestClusterPluginInstallReplayAndDetachedArtifact(t *testing.T) {
	t.Parallel()
	env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
	protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{}, env.snapshot(t))
	request := clusterPluginRequest()
	expected := common.CloneProto(request.GetPlugin())
	expected.Compatible, expected.Message, expected.Descriptors[0].Supported = false, "", false

	response, err := env.handler.InstallClusterPlugin(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, response.GetPlugin())
	request.GetPlugin().Descriptors[0].ConfigurationSchema.Fields["type"] = structpb.NewStringValue("changed")
	response.GetPlugin().Version = "changed"
	protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{
		Generation: 1, Plugins: []*contractpb.Plugin{expected},
	}, env.snapshot(t))

	request.Resolution = &registrypb.InstallClusterPluginRequest_Rejection{Rejection: &registrypb.Rejection{
		Code: registrypb.REJECTION_CODE_NOT_FOUND, Message: "configured release no longer exists",
	}}
	retry, err := env.handler.InstallClusterPlugin(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, retry.GetPlugin())
	require.Len(t, env.state(t).Receipts, 1)

	request.Request.RequestId = "reinstall"
	request.Resolution = nil
	reinstalled, err := env.handler.InstallClusterPlugin(env.ctx, request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, expected, reinstalled.GetPlugin())
	require.Len(t, env.state(t).Receipts, 2)
	protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{
		Generation: 1, Plugins: []*contractpb.Plugin{expected},
	}, env.snapshot(t))
}

func TestClusterPluginRequestIdentityRejectsChangedContent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*contractpb.InstallPluginRequest)
	}{
		{"catalog", func(r *contractpb.InstallPluginRequest) { r.CatalogId = "other" }},
		{"plugin", func(r *contractpb.InstallPluginRequest) { r.PluginId = "other" }},
		{"version", func(r *contractpb.InstallPluginRequest) { r.Version = "2.0.0" }},
		{"digest", func(r *contractpb.InstallPluginRequest) { r.Digest = "sha256:" + strings.Repeat("b", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
			request := clusterPluginRequest()
			_, err := env.handler.InstallClusterPlugin(env.ctx, request)
			require.NoError(t, err)
			before := env.state(t)
			tc.change(request.Request)
			_, err = env.handler.InstallClusterPlugin(env.ctx, request)
			require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, before, env.state(t))
		})
	}
}

func TestClusterPluginSemanticRejectionIsPermanent(t *testing.T) {
	t.Parallel()
	env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
	request := clusterPluginRequest()
	request.Resolution = &registrypb.InstallClusterPluginRequest_Rejection{Rejection: &registrypb.Rejection{
		Code: registrypb.REJECTION_CODE_FAILED_PRECONDITION, Message: "cluster plugin acceptance is disabled",
	}}
	_, err := env.handler.InstallClusterPlugin(env.ctx, request)
	require.ErrorContains(t, err, "acceptance is disabled")
	before := env.state(t)
	request.Resolution = clusterPluginRequest().Resolution
	_, err = env.handler.InstallClusterPlugin(env.ctx, request)
	require.ErrorContains(t, err, "acceptance is disabled")
	protorequire.ProtoEqual(t, before, env.state(t))
	protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{}, env.snapshot(t))
	receipt, err := env.handler.ReadClusterPlugins(env.ctx, &registrypb.ReadClusterPluginsRequest{
		NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID,
		Operation: &registrypb.ReadClusterPluginsRequest_RequestId{RequestId: request.Request.RequestId},
	})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, before.Receipts[request.Request.RequestId], receipt.Receipt)
}

func TestClusterPluginCapacityRefusalIsAtomic(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		change  func(*registrypb.ClusterPluginLimits, *registrypb.InstallClusterPluginRequest)
		receipt bool
	}{
		{"installation count", func(l *registrypb.ClusterPluginLimits, _ *registrypb.InstallClusterPluginRequest) {
			l.Installations = 1
		}, true},
		{"permanent request count", func(l *registrypb.ClusterPluginLimits, _ *registrypb.InstallClusterPluginRequest) {
			l.Requests = 1
		}, false},
		{"complete retained state", func(l *registrypb.ClusterPluginLimits, r *registrypb.InstallClusterPluginRequest) {
			l.StateBytes = 4096
			r.GetPlugin().Provenance = strings.Repeat("p", 4096)
		}, false},
		{"metadata size", func(_ *registrypb.ClusterPluginLimits, r *registrypb.InstallClusterPluginRequest) {
			r.GetPlugin().Provenance = strings.Repeat("p", 64<<10)
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			limits := defaultClusterPluginLimits()
			rejected := clusterPluginRequest()
			rejected.Request.RequestId = "second"
			rejected.Request.Digest = "sha256:" + strings.Repeat("b", 64)
			rejected.GetPlugin().Digest = rejected.Request.Digest
			tc.change(limits, rejected)
			env := newClusterPluginEnvironment(t, limits)
			first := clusterPluginRequest()
			accepted, err := env.handler.InstallClusterPlugin(env.ctx, first)
			require.NoError(t, err)
			before := env.state(t)
			_, err = env.handler.InstallClusterPlugin(env.ctx, rejected)
			require.Equal(t, codes.ResourceExhausted, serviceerror.ToStatus(err).Code())
			after := env.state(t)
			if tc.receipt {
				require.NotNil(t, after.Receipts["second"].GetRejection())
				delete(after.Receipts, "second")
			}
			protorequire.ProtoEqual(t, before, after)
			retry, err := env.handler.InstallClusterPlugin(env.ctx, first)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, accepted, retry)
		})
	}
}

func TestClusterPluginDigestCannotChangeSelection(t *testing.T) {
	t.Parallel()
	env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
	request := clusterPluginRequest()
	_, err := env.handler.InstallClusterPlugin(env.ctx, request)
	require.NoError(t, err)
	before := env.snapshot(t)
	request.Request.RequestId, request.Request.CatalogId = "another-selection", "another-catalog"
	request.GetPlugin().CatalogId = request.Request.CatalogId
	_, err = env.handler.InstallClusterPlugin(env.ctx, request)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.snapshot(t))
	request.Resolution = nil
	_, err = env.handler.InstallClusterPlugin(env.ctx, request)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
}

func TestClusterPluginSystemBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*registrypb.InstallClusterPluginRequest)
		code   codes.Code
	}{
		{"foreign namespace", func(r *registrypb.InstallClusterPluginRequest) {
			r.NamespaceId = "00000000-0000-4000-8000-000000000001"
		}, codes.InvalidArgument},
		{"namespace registry key", func(r *registrypb.InstallClusterPluginRequest) { r.ExecutionId = BusinessID }, codes.InvalidArgument},
		{"missing public request", func(r *registrypb.InstallClusterPluginRequest) { r.Request = nil }, codes.InvalidArgument},
		{"unbounded envelope", func(r *registrypb.InstallClusterPluginRequest) {
			r.GetPlugin().Provenance = strings.Repeat("p", 128<<10)
		}, codes.InvalidArgument},
		{"changed resolved digest", func(r *registrypb.InstallClusterPluginRequest) {
			r.GetPlugin().Digest = "sha256:" + strings.Repeat("b", 64)
		}, codes.DataLoss},
		{"missing resolution", func(r *registrypb.InstallClusterPluginRequest) { r.Resolution = nil }, codes.DataLoss},
		{"missing rejection", func(r *registrypb.InstallClusterPluginRequest) {
			r.Resolution = &registrypb.InstallClusterPluginRequest_Rejection{}
		}, codes.InvalidArgument},
		{"transient rejection", func(r *registrypb.InstallClusterPluginRequest) {
			r.Resolution = &registrypb.InstallClusterPluginRequest_Rejection{Rejection: &registrypb.Rejection{
				Code: registrypb.RejectionCode(codes.Unavailable), Message: "not reachable",
			}}
		}, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
			request := clusterPluginRequest()
			tc.change(request)
			_, err := env.handler.InstallClusterPlugin(env.ctx, request)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{}, env.snapshot(t))
		})
	}
}

func TestClusterPluginReadBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*registrypb.ReadClusterPluginsRequest)
		code   codes.Code
	}{
		{"missing selector", func(r *registrypb.ReadClusterPluginsRequest) { r.Operation = nil }, codes.InvalidArgument},
		{"false snapshot", func(r *registrypb.ReadClusterPluginsRequest) {
			r.Operation = &registrypb.ReadClusterPluginsRequest_Snapshot{}
		}, codes.InvalidArgument},
		{"empty identity", func(r *registrypb.ReadClusterPluginsRequest) {
			r.Operation = &registrypb.ReadClusterPluginsRequest_RequestId{}
		}, codes.InvalidArgument},
		{"unbounded identity", func(r *registrypb.ReadClusterPluginsRequest) {
			r.Operation = &registrypb.ReadClusterPluginsRequest_RequestId{RequestId: strings.Repeat("x", 129)}
		}, codes.InvalidArgument},
		{"foreign namespace", func(r *registrypb.ReadClusterPluginsRequest) {
			r.NamespaceId = "00000000-0000-4000-8000-000000000001"
		}, codes.InvalidArgument},
		{"namespace registry key", func(r *registrypb.ReadClusterPluginsRequest) { r.ExecutionId = BusinessID }, codes.InvalidArgument},
		{"missing identity", func(r *registrypb.ReadClusterPluginsRequest) {
			r.Operation = &registrypb.ReadClusterPluginsRequest_RequestId{RequestId: "unknown"}
		}, codes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
			request := &registrypb.ReadClusterPluginsRequest{
				NamespaceId: primitives.SystemNamespaceID, ExecutionId: ClusterPluginBusinessID,
				Operation: &registrypb.ReadClusterPluginsRequest_Snapshot{Snapshot: true},
			}
			tc.change(request)
			_, err := env.handler.ReadClusterPlugins(env.ctx, request)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{}, env.snapshot(t))
			_, err = env.handler.InstallClusterPlugin(env.ctx, clusterPluginRequest())
			require.NoError(t, err)
			before := env.state(t)
			_, err = env.handler.ReadClusterPlugins(env.ctx, request)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			protorequire.ProtoEqual(t, before, env.state(t))
		})
	}
}
