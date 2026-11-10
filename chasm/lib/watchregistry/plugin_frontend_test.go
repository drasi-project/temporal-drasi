package watchregistry

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	watchclient "go.temporal.io/server/client/watch"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type pluginFrontendObserver struct {
	pluginObserver
	get     func(context.Context, *contractpb.GetPluginRequest) (*contractpb.Plugin, error)
	list    func(context.Context, *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error)
	observe func(context.Context, uint64, []string) (map[string][]*contractpb.PluginHostStatus, error)
}

func (o pluginFrontendObserver) GetPlugin(ctx context.Context, request *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
	return o.get(ctx, request)
}

func (o pluginFrontendObserver) ListCatalogs(ctx context.Context, request *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error) {
	return o.list(ctx, request)
}

func (o pluginFrontendObserver) ObservePlugins(ctx context.Context, generation uint64, digests []string) (map[string][]*contractpb.PluginHostStatus, error) {
	return o.observe(ctx, generation, digests)
}

type pluginFrontendHistory struct {
	registrypb.ClusterPluginServiceClient
	server registrypb.ClusterPluginServiceServer
}

func (c pluginFrontendHistory) ReadClusterPlugins(ctx context.Context, request *registrypb.ReadClusterPluginsRequest, _ ...grpc.CallOption) (*registrypb.ReadClusterPluginsResponse, error) {
	return c.server.ReadClusterPlugins(ctx, request)
}

func (c pluginFrontendHistory) InstallClusterPlugin(ctx context.Context, request *registrypb.InstallClusterPluginRequest, _ ...grpc.CallOption) (*registrypb.InstallClusterPluginResponse, error) {
	return c.server.InstallClusterPlugin(ctx, request)
}

func (c pluginFrontendHistory) LoadClusterPlugin(ctx context.Context, request *registrypb.LoadClusterPluginRequest, _ ...grpc.CallOption) (*registrypb.LoadClusterPluginResponse, error) {
	return c.server.LoadClusterPlugin(ctx, request)
}

func unknownPluginObservations(_ context.Context, _ uint64, digests []string) (map[string][]*contractpb.PluginHostStatus, error) {
	result := make(map[string][]*contractpb.PluginHostStatus, len(digests))
	for _, digest := range digests {
		result[digest] = []*contractpb.PluginHostStatus{{
			HostId: "watch-member", Message: "Actual native verification has not completed",
		}}
	}
	return result, nil
}

func publicPluginEnvironment(t *testing.T, observer pluginFrontendObserver) (*pluginFrontendHandler, *clusterPluginEnvironment) {
	t.Helper()
	env := newClusterPluginEnvironment(t, defaultClusterPluginLimits())
	if observer.observe == nil {
		observer.observe = unknownPluginObservations
	}
	return &pluginFrontendHandler{
		client: pluginFrontendHistory{server: env.handler}, observer: observer,
		authorizer: authorization.NewDefaultAuthorizer(), logger: log.NewNoopLogger(),
	}, env
}

func TestPublicPluginInstallReplaysBeforeCatalogAccess(t *testing.T) {
	t.Parallel()
	request := clusterPluginRequest()
	calls := 0
	available := true
	h, env := publicPluginEnvironment(t, pluginFrontendObserver{
		get: func(context.Context, *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
			calls++
			if !available {
				return nil, serviceerror.NewNotFound("catalog release was removed")
			}
			return common.CloneProto(request.GetPlugin()), nil
		},
	})
	first, err := h.InstallPlugin(env.ctx, request.Request)
	require.NoError(t, err)
	require.False(t, first.Plugin.Compatible)
	require.False(t, first.Plugin.Descriptors[0].Supported)
	require.Equal(t, contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED, first.Hosts[0].State)
	require.Empty(t, first.Hosts[0].VerifiedSignerIdentity)
	before := env.state(t)
	available = false
	replay, err := h.InstallPlugin(env.ctx, request.Request)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, first, replay)
	protorequire.ProtoEqual(t, before, env.state(t))
	require.Equal(t, 1, calls)

	changed := common.CloneProto(request.Request)
	changed.Version = "2.0.0"
	_, err = h.InstallPlugin(env.ctx, changed)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	_, err = h.LoadPlugin(env.ctx, &contractpb.LoadPluginRequest{
		Digest: request.Request.Digest, RequestId: request.Request.RequestId,
	})
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	require.Equal(t, 1, calls)
	protorequire.ProtoEqual(t, before, env.state(t))
}

func TestPublicPluginInstallRetainsSemanticRefusalsOnly(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"missing release", serviceerror.NewNotFound("configured release not found"), codes.NotFound},
		{"changed digest", nil, codes.FailedPrecondition},
		{"unavailable catalog", serviceerror.NewUnavailable("catalog unavailable"), codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			request := clusterPluginRequest()
			calls := 0
			artifact := common.CloneProto(request.GetPlugin())
			artifact.Digest = "sha256:" + strings.Repeat("b", 64)
			resolveErr := tc.err
			h, env := publicPluginEnvironment(t, pluginFrontendObserver{
				get: func(context.Context, *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
					calls++
					return artifact, resolveErr
				},
			})
			result, err := h.InstallPlugin(env.ctx, request.Request)
			require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
			require.Nil(t, result)
			protorequire.ProtoEqual(t, &registrypb.ReadClusterPluginsResponse{}, env.snapshot(t))
			artifact, resolveErr = common.CloneProto(request.GetPlugin()), nil
			result, err = h.InstallPlugin(env.ctx, request.Request)
			if tc.code == codes.Unavailable {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, 2, calls)
			} else {
				require.Equal(t, tc.code, serviceerror.ToStatus(err).Code())
				require.Nil(t, result)
				require.Equal(t, 1, calls)
				require.Len(t, env.state(t).Receipts, 1)
			}
		})
	}
}

func TestPublicPluginObservationFailureDoesNotRepeatAcceptedInstallation(t *testing.T) {
	t.Parallel()
	request := clusterPluginRequest()
	calls := 0
	h, env := publicPluginEnvironment(t, pluginFrontendObserver{
		get: func(context.Context, *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
			calls++
			return common.CloneProto(request.GetPlugin()), nil
		},
		observe: func(context.Context, uint64, []string) (map[string][]*contractpb.PluginHostStatus, error) {
			return nil, serviceerror.NewUnavailable("membership unavailable")
		},
	})
	_, err := h.InstallPlugin(env.ctx, request.Request)
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	before := env.state(t)
	require.Len(t, before.Installations, 1)
	require.Len(t, before.Receipts, 1)
	h.observer = pluginFrontendObserver{observe: unknownPluginObservations}
	result, err := h.InstallPlugin(env.ctx, request.Request)
	require.NoError(t, err)
	require.Equal(t, request.Request.Digest, result.Plugin.Digest)
	require.Equal(t, 1, calls)
	protorequire.ProtoEqual(t, before, env.state(t))
}

func TestPublicPluginLoadReplayProjectsCurrentDesiredSelection(t *testing.T) {
	t.Parallel()
	first, second := clusterPluginRequest(), clusterPluginRequest()
	second.Request.RequestId, second.Request.Version = "install-second", "2.0.0"
	second.Request.Digest = "sha256:" + strings.Repeat("b", 64)
	second.GetPlugin().Version, second.GetPlugin().Digest = second.Request.Version, second.Request.Digest
	calls := 0
	h, env := publicPluginEnvironment(t, pluginFrontendObserver{
		get: func(_ context.Context, request *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
			calls++
			if request.Version == first.Request.Version {
				return common.CloneProto(first.GetPlugin()), nil
			}
			return common.CloneProto(second.GetPlugin()), nil
		},
	})
	loads := []*contractpb.LoadPluginRequest{
		{Digest: first.Request.Digest, RequestId: "load-first"},
		{Digest: second.Request.Digest, RequestId: "load-second"},
	}
	for i, install := range []*registrypb.InstallClusterPluginRequest{first, second} {
		_, err := h.InstallPlugin(env.ctx, install.Request)
		require.NoError(t, err)
		result, err := h.LoadPlugin(env.ctx, loads[i])
		require.NoError(t, err)
		require.True(t, result.LoadRequested)
		require.Equal(t, contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED, result.Hosts[0].State)
	}
	before := env.state(t)
	replay, err := h.LoadPlugin(env.ctx, loads[0])
	require.NoError(t, err)
	require.False(t, replay.LoadRequested)
	require.Equal(t, first.Request.Digest, replay.Plugin.Digest)
	require.Equal(t, 2, calls)
	protorequire.ProtoEqual(t, before, env.state(t))
	installation, err := h.GetInstallation(env.ctx, &contractpb.GetInstallationRequest{Digest: second.Request.Digest})
	require.NoError(t, err)
	require.True(t, installation.LoadRequested)
}

func TestPublicPluginMissingLoadRemainsRejectedAfterInstallation(t *testing.T) {
	t.Parallel()
	request := clusterPluginRequest()
	h, env := publicPluginEnvironment(t, pluginFrontendObserver{
		get: func(context.Context, *contractpb.GetPluginRequest) (*contractpb.Plugin, error) {
			return common.CloneProto(request.GetPlugin()), nil
		},
	})
	load := &contractpb.LoadPluginRequest{Digest: request.Request.Digest, RequestId: "load-before-install"}
	_, err := h.LoadPlugin(env.ctx, load)
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	_, err = h.InstallPlugin(env.ctx, request.Request)
	require.NoError(t, err)
	before := env.state(t)
	_, err = h.LoadPlugin(env.ctx, load)
	require.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code())
	protorequire.ProtoEqual(t, before, env.state(t))
}

type pluginSnapshotHistory struct {
	registrypb.ClusterPluginServiceClient
	snapshot *registrypb.ReadClusterPluginsResponse
}

func (c pluginSnapshotHistory) ReadClusterPlugins(context.Context, *registrypb.ReadClusterPluginsRequest, ...grpc.CallOption) (*registrypb.ReadClusterPluginsResponse, error) {
	return common.CloneProto(c.snapshot), nil
}

func TestPublicPluginPaginationReservesCompleteHostEvidence(t *testing.T) {
	t.Parallel()
	snapshot := &registrypb.ReadClusterPluginsResponse{Generation: 10}
	for i := range 10 {
		snapshot.Plugins = append(snapshot.Plugins, &contractpb.Plugin{
			CatalogId: "trusted", Id: "plugin", Version: fmt.Sprintf("1.0.%d", i),
			Digest: fmt.Sprintf("sha256:%064x", i),
			Descriptors: []*contractpb.PluginDescriptor{{
				Component: "source", Category: contractpb.PluginCategory_PLUGIN_CATEGORY_SOURCE,
				ConfigurationSchema: &structpb.Struct{Fields: map[string]*structpb.Value{
					"description": structpb.NewStringValue(strings.Repeat("m", 60<<10)),
				}},
			}},
		})
	}
	h := &pluginFrontendHandler{
		client: pluginSnapshotHistory{snapshot: snapshot},
		observer: pluginFrontendObserver{
			observe: func(_ context.Context, _ uint64, digests []string) (map[string][]*contractpb.PluginHostStatus, error) {
				require.LessOrEqual(t, len(digests), 3)
				result := make(map[string][]*contractpb.PluginHostStatus, len(digests))
				for _, digest := range digests {
					for i := range watchclient.MaxPluginHosts {
						host := &contractpb.PluginHostStatus{
							HostId: fmt.Sprintf("member-%02d", i), Message: strings.Repeat("x", 4000),
						}
						require.Less(t, proto.Size(host), watchclient.MaxPluginHostStatusBytes)
						result[digest] = append(result[digest], host)
					}
				}
				return result, nil
			},
		},
	}
	request := &contractpb.ListInstallationsRequest{}
	var seen []string
	var continuation string
	for {
		page, err := h.ListInstallations(t.Context(), request)
		require.NoError(t, err)
		require.LessOrEqual(t, proto.Size(page), 1<<20)
		for _, installation := range page.Installations {
			seen = append(seen, installation.Plugin.Digest)
			require.Len(t, installation.Hosts, watchclient.MaxPluginHosts)
		}
		if page.NextPageToken == "" {
			break
		}
		continuation = page.NextPageToken
		request.PageToken = continuation
	}
	require.Len(t, seen, len(snapshot.Plugins))
	for i, plugin := range snapshot.Plugins {
		require.Equal(t, plugin.Digest, seen[i])
	}
	request.PageToken = continuation
	snapshot.Generation++
	_, err := h.ListInstallations(t.Context(), request)
	require.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code())
	request.PageToken = "not-a-page-token"
	_, err = h.ListInstallations(t.Context(), request)
	require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func TestPublicPluginEmptyMembershipIsNotReadiness(t *testing.T) {
	t.Parallel()
	artifact := common.CloneProto(clusterPluginRequest().GetPlugin())
	artifact.Compatible, artifact.Message, artifact.Descriptors[0].Supported = false, "", false
	h := &pluginFrontendHandler{
		client: pluginSnapshotHistory{snapshot: &registrypb.ReadClusterPluginsResponse{
			Generation: 1, Plugins: []*contractpb.Plugin{artifact},
		}},
		observer: pluginFrontendObserver{
			observe: func(context.Context, uint64, []string) (map[string][]*contractpb.PluginHostStatus, error) {
				return nil, nil
			},
		},
	}
	_, err := h.GetInstallation(t.Context(), &contractpb.GetInstallationRequest{Digest: artifact.Digest})
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
	_, err = h.ListInstallations(t.Context(), &contractpb.ListInstallationsRequest{})
	require.Equal(t, codes.Unavailable, serviceerror.ToStatus(err).Code())
}

func TestPublicPluginCapabilitiesAreClusterScoped(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		claims *authorization.Claims
	}{
		{"anonymous", nil},
		{"namespace administrator", &authorization.Claims{Namespaces: map[string]authorization.Role{"test": authorization.RoleAdmin}}},
		{"cluster reader", &authorization.Claims{System: authorization.RoleReader}},
		{"cluster writer", &authorization.Claims{System: authorization.RoleWriter}},
		{"cluster administrator", &authorization.Claims{System: authorization.RoleAdmin}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := &pluginFrontendHandler{
				observer: pluginFrontendObserver{
					list: func(context.Context, *contractpb.ListCatalogsRequest) (*contractpb.ListCatalogsResponse, error) {
						return &contractpb.ListCatalogsResponse{}, nil
					},
				},
				authorizer: authorization.NewDefaultAuthorizer(), logger: log.NewNoopLogger(),
			}

			ctx := context.WithValue(t.Context(), authorization.MappedClaims, tc.claims)
			result, err := h.GetCapabilities(ctx, &contractpb.GetPluginCapabilitiesRequest{})
			require.NoError(t, err)
			expected := &contractpb.PluginCapabilities{
				Enabled: true, ApiVersion: "v1", MaxRequestBytes: 1 << 20,
				Message: "Installation and loading record durable intent; inspect every host for actual verification and loading",
			}
			if tc.claims != nil && tc.claims.System >= authorization.RoleReader {
				expected.AllowedMethods = slices.Clone(pluginMethods)
				if tc.claims.System < authorization.RoleAdmin {
					expected.AllowedMethods = slices.DeleteFunc(expected.AllowedMethods, func(method string) bool {
						return method == contractpb.PluginService_InstallPlugin_FullMethodName ||
							method == contractpb.PluginService_LoadPlugin_FullMethodName
					})
				}
			}
			protorequire.ProtoEqual(t, expected, result)
		})
	}
}

func TestPublicPluginCapabilityAuthorizationFailureReturnsNoResult(t *testing.T) {
	t.Parallel()
	authorizer := authorization.NewMockAuthorizer(gomock.NewController(t))
	authorizer.EXPECT().Authorize(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(authorization.Result{}, errors.New("authorizer unavailable"))
	h := &pluginFrontendHandler{authorizer: authorizer, logger: log.NewNoopLogger()}
	result, err := h.GetCapabilities(t.Context(), &contractpb.GetPluginCapabilitiesRequest{})
	require.Equal(t, codes.Internal, serviceerror.ToStatus(err).Code())
	require.Nil(t, result)
}

func TestPublicPluginDisabledCapabilitiesDoNotAdvertiseMutations(t *testing.T) {
	t.Parallel()
	h := &pluginFrontendHandler{authorizer: authorization.NewDefaultAuthorizer(), logger: log.NewNoopLogger()}
	ctx := context.WithValue(t.Context(), authorization.MappedClaims, &authorization.Claims{System: authorization.RoleAdmin})
	result, err := h.GetCapabilities(ctx, &contractpb.GetPluginCapabilitiesRequest{})
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &contractpb.PluginCapabilities{
		ApiVersion: "v1", MaxRequestBytes: 1 << 20, Message: "Plugin management is unavailable",
		AllowedMethods: []string{contractpb.PluginService_GetCapabilities_FullMethodName},
	}, result)
}
