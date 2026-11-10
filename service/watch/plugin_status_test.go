package watch

import (
	"context"
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/watchservice/v1"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/watch/native"
)

func pluginStatusService(t *testing.T) (*Service, string, namespace.ID) {
	t.Helper()
	view, err := parsePluginIntent(pluginSnapshot())
	require.NoError(t, err)
	digest := view.snapshot.Plugins[0].Digest
	now := time.Now()
	controller := func() *runtimePlugins {
		return &runtimePlugins{
			view: view, done: make(chan struct{}),
			observations: map[string]runtimePluginObservation{
				digest: {
					generation: view.snapshot.Generation, observedAt: now, state: native.PluginLoaded,
				},
			},
		}
	}
	service := &Service{
		bootID:     "20000000-0000-4000-8000-000000000001",
		config:     config.Watch{StorageHostID: "10000000-0000-4000-8000-000000000001"},
		pluginView: view, pluginObservedAt: now, plugins: controller(),
	}
	id := namespace.ID("00000000-0000-4000-8000-000000000001")
	service.namespaces = map[namespace.ID]*hostedNamespace{id: {
		graph: &Graph{owner: &assignmentpb.Active{Grant: &assignmentpb.Grant{
			NamespaceId: id.String(), InstanceId: id.String(), Epoch: 1,
			ProcessBootId: service.bootID, StorageHostId: service.config.StorageHostID,
		}}, plugins: controller()},
	}}
	return service, digest, id
}

func TestExternalSourcesRequireReviewedLoadedPair(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		change    func(*Service, string, string, namespace.ID)
		available bool
	}{
		{name: "unsigned components loaded everywhere", available: true},
		{name: "no active namespace graphs", available: true, change: func(s *Service, _, _ string, _ namespace.ID) {
			clear(s.namespaces)
		}},
		{name: "two roles share an artifact", available: true, change: func(s *Service, source, _ string, _ namespace.ID) {
			s.params.PluginPolicy.ManagedPlugins[0].Bootstrap.Digest = source
		}},
		{name: "one complete pair suffices", available: true, change: func(s *Service, _, _ string, _ namespace.ID) {
			s.params.PluginPolicy.ManagedPlugins = append(s.params.PluginPolicy.ManagedPlugins, native.ManagedPluginPair{
				Source: native.PluginComponentRef{Digest: "sha256:" + strings.Repeat("c", 64), Component: "other"},
			})
		}},
		{name: "no credential reader", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.params.Credentials = nil
		}},
		{name: "no administrator policy", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.params.PluginPolicy = nil
		}},
		{name: "independently loaded artifacts are not a reviewed pair", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.params.PluginPolicy.ManagedPlugins = nil
		}},
		{name: "reviewed bootstrap is not retained", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.params.PluginPolicy.ManagedPlugins[0].Bootstrap.Digest = "sha256:" + strings.Repeat("c", 64)
		}},
		{name: "bootstrap not desired loaded", change: func(s *Service, _, bootstrap string, _ namespace.ID) {
			delete(s.pluginView.snapshot.DesiredLoads, s.pluginView.byDigest[bootstrap].Id)
		}},
		{name: "bootstrap installed only", change: func(s *Service, _, bootstrap string, _ namespace.ID) {
			observation := s.plugins.observations[bootstrap]
			observation.state = native.PluginInstalled
			s.plugins.observations[bootstrap] = observation
		}},
		{name: "bootstrap incompatible", change: func(s *Service, _, bootstrap string, _ namespace.ID) {
			observation := s.plugins.observations[bootstrap]
			observation.state, observation.problem = native.PluginIncompatible, "embedded version mismatch"
			s.plugins.observations[bootstrap] = observation
		}},
		{name: "graph missing bootstrap evidence", change: func(s *Service, _, bootstrap string, id namespace.ID) {
			delete(s.namespaces[id].graph.plugins.observations, bootstrap)
		}},
		{name: "graph bootstrap evidence expired", change: func(s *Service, _, bootstrap string, id namespace.ID) {
			controller := s.namespaces[id].graph.plugins
			observation := controller.observations[bootstrap]
			observation.observedAt = time.Now().Add(-3 * time.Minute)
			controller.observations[bootstrap] = observation
		}},
		{name: "graph bootstrap evidence from future", change: func(s *Service, _, bootstrap string, id namespace.ID) {
			controller := s.namespaces[id].graph.plugins
			observation := controller.observations[bootstrap]
			observation.observedAt = time.Now().Add(time.Minute)
			controller.observations[bootstrap] = observation
		}},
		{name: "graph at another generation", change: func(s *Service, _, _ string, id namespace.ID) {
			view := *s.pluginView
			view.snapshot = common.CloneProto(view.snapshot)
			view.snapshot.Generation++
			s.namespaces[id].graph.plugins.view = &view
		}},
		{name: "graph closing", change: func(s *Service, _, _ string, id namespace.ID) {
			s.namespaces[id].graph.closing = make(chan struct{})
		}},
		{name: "new graph not loaded", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.namespaces["00000000-0000-4000-8000-000000000002"] = &hostedNamespace{}
		}},
		{name: "stale root", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.pluginObservedAt = time.Now().Add(-time.Minute)
		}},
		{name: "future root", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.pluginObservedAt = time.Now().Add(time.Minute)
		}},
		{name: "missing root", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.pluginView = nil
		}},
		{name: "unavailable History", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.pluginProblem = "History unavailable"
		}},
		{name: "management absent", change: func(s *Service, _, _ string, _ namespace.ID) {
			s.plugins = nil
		}},
		{name: "management closed", change: func(s *Service, _, _ string, _ namespace.ID) {
			close(s.plugins.done)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service, source, id := pluginStatusService(t)
			bootstrap := "sha256:" + strings.Repeat("b", 64)
			snapshot := common.CloneProto(service.pluginView.snapshot)
			plugin := common.CloneProto(snapshot.Plugins[0])
			plugin.Id, plugin.Digest = "fixture-bootstrap", bootstrap
			snapshot.Plugins = append(snapshot.Plugins, plugin)
			snapshot.DesiredLoads[plugin.Id] = bootstrap
			view, err := parsePluginIntent(snapshot)
			require.NoError(t, err)
			service.pluginView = view
			for _, controller := range []*runtimePlugins{service.plugins, service.namespaces[id].graph.plugins} {
				controller.view = view
				controller.observations[bootstrap] = controller.observations[source]
			}
			service.params.PluginPolicy = &PluginPolicy{ManagedPlugins: []native.ManagedPluginPair{{
				Source:    native.PluginComponentRef{Digest: source, Component: "source"},
				Bootstrap: native.PluginComponentRef{Digest: bootstrap, Component: "bootstrap"},
			}}}
			reads := make(chan struct{}, 1)
			service.params.Credentials = graphCredentialReader(func(context.Context, namespace.ID, native.SecretReference) ([]byte, error) {
				select {
				case reads <- struct{}{}:
				default:
				}
				return nil, context.Canceled
			})
			if test.change != nil {
				test.change(service, source, bootstrap, id)
			}
			available, err := service.externalSourcesAvailable(t.Context())
			require.NoError(t, err)
			require.Equal(t, test.available, available)
			require.Empty(t, reads, "capability observation must not resolve credentials")
		})
	}
}

func TestExternalSourceCapabilityHonorsCancellation(t *testing.T) {
	t.Parallel()
	service, _, _ := pluginStatusService(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	available, err := service.externalSourcesAvailable(ctx)
	require.False(t, available)
	require.ErrorIs(t, err, context.Canceled)
}

func TestObservePluginsIncludesEveryActualGraph(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*Service, namespace.ID)
		loaded bool
	}{
		{name: "all current runtimes", loaded: true},
		{name: "new graph opening", change: func(s *Service, id namespace.ID) { s.namespaces[id].graph = nil }},
		{name: "graph awaiting intent", change: func(s *Service, id namespace.ID) { s.namespaces[id].graph.plugins = nil }},
		{name: "graph startup failed", change: func(s *Service, id namespace.ID) { s.namespaces[id].problem = "open failed" }},
		{name: "graph closing", change: func(s *Service, id namespace.ID) { s.namespaces[id].graph.closing = make(chan struct{}) }},
		{name: "failed closed graph retained", change: func(s *Service, id namespace.ID) { s.namespaces[id].closed = true }},
		{name: "graph from another boot", change: func(s *Service, id namespace.ID) { s.namespaces[id].graph.owner.Grant.ProcessBootId = "other-boot" }},
		{name: "graph from another volume", change: func(s *Service, id namespace.ID) { s.namespaces[id].graph.owner.Grant.StorageHostId = "other-volume" }},
		{name: "second graph not ready", change: func(s *Service, _ namespace.ID) {
			s.namespaces["00000000-0000-4000-8000-000000000002"] = &hostedNamespace{}
		}},
		{name: "no applicable graphs", loaded: true, change: func(s *Service, _ namespace.ID) { clear(s.namespaces) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service, digest, id := pluginStatusService(t)
			if test.change != nil {
				test.change(service, id)
			}
			response, err := service.ObservePlugins(t.Context(), &watchservice.ObservePluginsRequest{Generation: 2, Digests: []string{digest}})
			require.NoError(t, err)
			require.Equal(t, uint64(2), response.Generation)
			require.Equal(t, service.bootID, response.ProcessBootId)
			require.Equal(t, service.config.StorageHostID, response.StorageHostId)
			require.Len(t, response.Observations, 1)
			require.Equal(t, digest, response.Observations[0].Digest)
			require.Equal(t, test.loaded, response.Observations[0].Status.State == contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED)
		})
	}
}

func TestObservePluginsRefusesUnavailableIntent(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*Service)
	}{
		{"missing view", func(s *Service) { s.pluginView = nil }},
		{"older cut", func(s *Service) { s.pluginView.snapshot.Generation = 1 }},
		{"unavailable History is not an empty cut", func(s *Service) { s.pluginProblem = "History unavailable" }},
		{"stale root", func(s *Service) { s.pluginObservedAt = time.Now().Add(-6 * time.Second) }},
		{"future root", func(s *Service) { s.pluginObservedAt = time.Now().Add(3 * time.Second) }},
		{"management absent", func(s *Service) { s.plugins = nil }},
		{"management closed", func(s *Service) { close(s.plugins.done) }},
		{"host shutting down", func(s *Service) { s.pluginClosing = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service, digest, _ := pluginStatusService(t)
			test.change(service)
			response, err := service.ObservePlugins(t.Context(), &watchservice.ObservePluginsRequest{Generation: 2, Digests: []string{digest}})
			require.Nil(t, response)
			require.Error(t, err)
		})
	}
}

func TestObservePluginsRequestBoundaries(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		request *watchservice.ObservePluginsRequest
	}{
		{"nil request", nil},
		{"mutable digest", &watchservice.ObservePluginsRequest{Generation: 2, Digests: []string{"latest"}}},
		{"duplicate digest", &watchservice.ObservePluginsRequest{Generation: 2, Digests: []string{pluginSnapshot().Plugins[0].Digest, pluginSnapshot().Plugins[0].Digest}}},
		{"not retained", &watchservice.ObservePluginsRequest{Generation: 2, Digests: []string{"sha256:" + strings.Repeat("b", 64)}}},
		{"too many digests", &watchservice.ObservePluginsRequest{Generation: 2, Digests: make([]string, 201)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service, _, _ := pluginStatusService(t)
			response, err := service.ObservePlugins(t.Context(), test.request)
			require.Nil(t, response)
			require.Error(t, err)
		})
	}

	t.Run("readiness without artifacts", func(t *testing.T) {
		t.Parallel()
		service, _, _ := pluginStatusService(t)
		response, err := service.ObservePlugins(t.Context(), &watchservice.ObservePluginsRequest{Generation: 2})
		require.NoError(t, err)
		require.Empty(t, response.Observations)
	})
	t.Run("canceled caller", func(t *testing.T) {
		t.Parallel()
		service, digest, _ := pluginStatusService(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		response, err := service.ObservePlugins(ctx, &watchservice.ObservePluginsRequest{Generation: 2, Digests: []string{digest}})
		require.Nil(t, response)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestPluginObservationCutFence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		change  func(*Service, namespace.ID)
		current bool
	}{
		{name: "unchanged cut", current: true},
		{name: "root expired while sampling", change: func(s *Service, _ namespace.ID) {
			s.pluginObservedAt = time.Now().Add(-6 * time.Second)
		}},
		{name: "root unavailable while sampling", change: func(s *Service, _ namespace.ID) {
			s.pluginProblem = "History unavailable"
		}},
		{name: "root replaced", change: func(s *Service, _ namespace.ID) {
			replacement := *s.pluginView
			s.pluginView = &replacement
		}},
		{name: "new namespace", change: func(s *Service, _ namespace.ID) {
			s.namespaces["00000000-0000-4000-8000-000000000002"] = &hostedNamespace{}
		}},
		{name: "removed namespace", change: func(s *Service, id namespace.ID) {
			delete(s.namespaces, id)
		}},
		{name: "replaced graph", change: func(s *Service, id namespace.ID) {
			s.namespaces[id].graph = &Graph{}
		}},
		{name: "graph began closing", change: func(s *Service, id namespace.ID) {
			s.namespaces[id].graph.closing = make(chan struct{})
		}},
		{name: "controller replaced", change: func(s *Service, id namespace.ID) {
			s.namespaces[id].graph.plugins = &runtimePlugins{}
		}},
		{name: "host began closing", change: func(s *Service, _ namespace.ID) {
			s.pluginClosing = true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service, _, id := pluginStatusService(t)
			view := service.pluginView
			hosted := service.namespaces[id]
			graphs := map[namespace.ID]pluginGraphSample{id: {
				hosted: hosted, graph: hosted.graph, controller: hosted.graph.plugins,
			}}
			if test.change != nil {
				test.change(service, id)
			}
			require.Equal(t, test.current, service.pluginObservationCurrent(view, graphs))
		})
	}
}
