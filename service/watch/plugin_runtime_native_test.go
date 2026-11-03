//go:build temporal_with_drasi && cgo

package watch

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/server/api/watchservice/v1"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	registrypb "go.temporal.io/server/chasm/lib/watchregistry/gen/registrypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/proto"
)

func blockedRuntimePluginPolicy(t *testing.T) (PluginPolicy, <-chan struct{}, func()) {
	t.Helper()
	started, release := make(chan struct{}, 16), make(chan struct{})
	server := httptest.NewUnstartedServer(http.NotFoundHandler())
	server.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-t.Context().Done():
		}
		return nil, errors.New("plugin fixture released")
	}}
	server.StartTLS()
	t.Cleanup(server.Close)
	registry := strings.TrimPrefix(server.URL, "https://") + "/trusted"
	policy := PluginPolicy{
		Trust:    native.Trust{RegistryPrefixes: []string{registry}, Identities: []native.Identity{{Issuer: "issuer", Subject: "subject"}}},
		Catalogs: []native.CatalogConfig{{ID: "trusted"}},
	}
	for _, release := range []struct{ version, digest string }{{"1.0.0", "a"}, {"2.0.0", "b"}} {
		policy.Catalogs[0].Plugins = append(policy.Catalogs[0].Plugins, native.CatalogRelease{
			Version: release.version,
			Artifact: native.Plugin{
				ID: "fixture-source", Reference: registry + "/source@sha256:" + strings.Repeat(release.digest, 64),
				BinarySHA256: strings.Repeat("c", 64), Filename: "libdrasi_fixture.so",
			},
		})
	}
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	return policy, started, unblock
}

func TestPluginRuntimeFencesActualBlockedCompletion(t *testing.T) {
	t.Parallel()
	policy, started, release := blockedRuntimePluginPolicy(t)
	runtime, err := native.Open(t.Context(), native.Config{
		StateDir: filepath.Join(t.TempDir(), "runtime"), InstanceID: "management", OwnerEpoch: 1,
		StartMode: native.StartModeDormant, Catalogs: policy.Catalogs, Trust: policy.Trust,
	})
	require.NoError(t, err)
	controller := newRuntimePlugins(runtime, "management", false, log.NewNoopLogger())
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, controller.close(ctx))
		require.NoError(t, runtime.Close(ctx))
	})
	view, err := parsePluginIntent(pluginSnapshot())
	require.NoError(t, err)
	require.NoError(t, controller.update(view))
	await.Rcv(t, started)
	snapshot := pluginSnapshot()
	selected := common.CloneProto(snapshot.Plugins[0])
	selected.Version, selected.Digest = "2.0.0", "sha256:"+strings.Repeat("b", 64)
	snapshot.Plugins = append(snapshot.Plugins, selected)
	snapshot.Generation = 4
	snapshot.DesiredLoads[selected.Id] = selected.Digest
	next, err := parsePluginIntent(snapshot)
	require.NoError(t, err)
	require.NoError(t, controller.update(next))
	require.Empty(t, controller.observation(4, selected.Digest, time.Now()).state)
	result, err := runtime.Apply(t.Context(), native.ObserveNodesCommand{})
	require.NoError(t, err)
	require.Empty(t, result.Nodes)
	release()
	await.Require(t.Context(), t, func(t *await.T) {
		observed := controller.observation(4, selected.Digest, time.Now())
		require.Equal(t, native.PluginFailed, observed.state)
		require.NotEmpty(t, observed.problem)
	}, 10*time.Second, 10*time.Millisecond)
	controller.mu.Lock()
	_, obsolete := controller.observations[view.snapshot.Plugins[0].Digest]
	controller.mu.Unlock()
	require.False(t, obsolete, "a superseded namespace pin must not be reintroduced by old completion")
}

func TestGraphPluginWorkerPrecedesFirstReconcileAndDrainsBeforeCheckpoint(t *testing.T) {
	t.Parallel()
	policy, started, release := blockedRuntimePluginPolicy(t)
	var state *assignmentpb.State
	client := &graphAssignmentClient{
		describe: func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
			if state == nil {
				return nil, serviceerror.NewNotFound("no assignment")
			}
			return common.CloneProto(state), nil
		},
		begin: func(_ context.Context, request *assignmentpb.BeginRequest) (*assignmentpb.State, error) {
			state = graphActiveState(t, request)
			return common.CloneProto(state), nil
		},
		seal: func(_ context.Context, request *assignmentpb.SealRequest) (*assignmentpb.State, error) {
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
			if err != nil {
				return nil, err
			}
			fingerprint := sha256.Sum256(encoded)
			state = &assignmentpb.State{Version: request.ExpectedVersion + 1, Phase: &assignmentpb.State_Sealed{
				Sealed: &assignmentpb.Sealed{
					Predecessor: common.CloneProto(state.GetActive()), Checkpoint: common.CloneProto(request.Checkpoint),
					RequestId: request.RequestId, Fingerprint: fingerprint[:],
				},
			}}
			return common.CloneProto(state), nil
		},
	}
	options := graphTestOptions(t, client)
	options.Plugins = policy
	options.Registry = &desiredTestClient{}
	options.Control = &intakeControlClient{}
	options.Workflows = workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	graph, err := OpenGraph(t.Context(), options)
	require.NoError(t, err)
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if graph.plugins != nil {
			require.NoError(t, graph.plugins.close(ctx))
		}
		require.NoError(t, graph.runtime.Close(ctx))
	})
	plugins, err := parsePluginIntent(pluginSnapshot())
	require.NoError(t, err)
	require.NoError(t, graph.updatePlugins(plugins, log.NewNoopLogger()))
	await.Rcv(t, started)
	require.Nil(t, graph.cancel)
	view := &DesiredView{
		namespaceID: options.NamespaceID, namespace: options.Namespace, generation: 1,
		current: map[resourceKey]*registrypb.Record{}, revisions: map[definitionPin]*registrypb.Record{},
	}
	_, err = graph.Reconcile(t.Context(), view)
	require.NoError(t, err)
	await.Require(t.Context(), t, func(t *await.T) {
		graph.mu.Lock()
		observed, generation, initialized := graph.observed, graph.nodeGeneration, graph.history != nil
		graph.mu.Unlock()
		require.True(t, observed)
		require.Equal(t, uint64(1), generation)
		require.True(t, initialized)
	}, 5*time.Second, 10*time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- graph.Close(t.Context()) }()
	select {
	case <-graph.plugins.done:
	case <-t.Context().Done():
		t.Fatal("plugin worker did not join during graph close")
	}
	await.Require(t.Context(), t, func(t *await.T) {
		_, err := os.Stat(graphDirectory(options.StateRoot, options.NamespaceID, "closing", 1))
		require.NoError(t, err)
	}, 5*time.Second, 10*time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("graph close passed blocked native completion: %v", err)
	default:
	}
	_, err = os.Stat(graph.destination)
	require.ErrorIs(t, err, os.ErrNotExist)
	release()
	require.NoError(t, await.Rcv(t, closed))
	_, err = readGraphCheckpoint(state.GetSealed().Checkpoint)
	require.NoError(t, err)
	require.NoError(t, graph.updatePlugins(plugins, log.NewNoopLogger()))
	_, err = graph.runtime.Apply(t.Context(), native.ObserveNodesCommand{})
	require.ErrorIs(t, err, native.ErrClosed)
}

func TestServicePluginCutReachesCurrentAndNewNativeGraphs(t *testing.T) {
	t.Parallel()
	policy, started, release := blockedRuntimePluginPolicy(t)
	var runtimes []*native.Runtime
	var graphs []*Graph
	var management *runtimePlugins
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if management != nil {
			require.NoError(t, management.close(ctx))
		}
		for _, graph := range graphs {
			if graph.plugins != nil {
				require.NoError(t, graph.plugins.close(ctx))
			}
		}
		for _, runtime := range runtimes {
			require.NoError(t, runtime.Close(ctx))
		}
	})
	open := func(id string) *native.Runtime {
		runtime, err := native.Open(t.Context(), native.Config{
			InstanceID: id, OwnerEpoch: 1, StateDir: filepath.Join(t.TempDir(), "runtime"),
			StartMode: native.StartModeDormant, Catalogs: policy.Catalogs, Trust: policy.Trust,
		})
		require.NoError(t, err)
		runtimes = append(runtimes, runtime)
		return runtime
	}
	management = newRuntimePlugins(open("management"), "management", true, log.NewNoopLogger())
	service := &Service{
		params: ServiceParams{Logger: log.NewNoopLogger()},
		config: config.Watch{StorageHostID: "10000000-0000-4000-8000-000000000001"},
		bootID: "20000000-0000-4000-8000-000000000001", plugins: management,
		namespaces: make(map[namespace.ID]*hostedNamespace),
	}
	view, err := parsePluginIntent(pluginSnapshot())
	require.NoError(t, err)
	require.NoError(t, service.publishPluginIntent(view, time.Now()))
	await.Rcv(t, started)
	addGraph := func(id namespace.ID) *Graph {
		graph := &Graph{
			runtime: open(id.String()), options: GraphOptions{NamespaceID: id.String()},
			owner: &assignmentpb.Active{Grant: &assignmentpb.Grant{
				NamespaceId: id.String(), InstanceId: id.String(), Epoch: 1,
				StorageHostId: service.config.StorageHostID, ProcessBootId: service.bootID,
			}},
		}
		graphs = append(graphs, graph)
		service.namespaces[id] = &hostedNamespace{graph: graph}
		return graph
	}
	first := addGraph("00000000-0000-4000-8000-000000000001")
	require.NoError(t, service.publishPluginIntent(view, time.Now()))
	await.Rcv(t, started)
	second := addGraph("00000000-0000-4000-8000-000000000002")
	require.NoError(t, service.publishPluginIntent(view, time.Now()))
	await.Rcv(t, started)
	require.NotSame(t, management, first.plugins)
	require.NotSame(t, first.plugins, second.plugins)
	release()
	digest := view.snapshot.Plugins[0].Digest
	await.Require(t.Context(), t, func(t *await.T) {
		for _, controller := range []*runtimePlugins{management, first.plugins, second.plugins} {
			observed := controller.observation(2, digest, time.Now())
			require.Equal(t, native.PluginFailed, observed.state)
			require.NotEmpty(t, observed.problem)
		}
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, service.publishPluginIntent(view, time.Now()))
	response, err := service.ObservePlugins(t.Context(), &watchservice.ObservePluginsRequest{Generation: 2, Digests: []string{digest}})
	require.NoError(t, err)
	require.Equal(t, contractpb.PluginHostState_PLUGIN_HOST_STATE_FAILED, response.Observations[0].Status.State)
	older, err := parsePluginIntent(&registrypb.ReadClusterPluginsResponse{})
	require.NoError(t, err)
	require.ErrorContains(t, service.publishPluginIntent(older, time.Now()), "regressed")
	require.Same(t, view, service.pluginView)
}
