package watch

import (
	"strings"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimePluginTargetsAndFencing(t *testing.T) {
	t.Parallel()
	for _, management := range []bool{false, true} {
		t.Run(map[bool]string{false: "namespace", true: "management"}[management], func(t *testing.T) {
			t.Parallel()
			snapshot := pluginSnapshot()
			old := common.CloneProto(snapshot.Plugins[0])
			selected := common.CloneProto(old)
			selected.Version, selected.Digest = "2.0.0", "sha256:"+strings.Repeat("b", 64)
			snapshot.Plugins = append(snapshot.Plugins, selected)
			snapshot.Generation = 4
			snapshot.DesiredLoads[selected.Id] = selected.Digest
			view, err := parsePluginIntent(snapshot)
			require.NoError(t, err)
			controller := &runtimePlugins{management: management, wake: make(chan struct{}, 1)}
			require.NoError(t, controller.update(view))
			expected := []string{selected.Digest}
			if management {
				expected = []string{old.Digest, selected.Digest}
			}
			now := time.Unix(100, 0)
			for _, digest := range expected {
				cut, target := controller.nextPlugin(now)
				require.Same(t, view, cut)
				require.Equal(t, digest, target.Digest)
				controller.observations[digest] = runtimePluginObservation{
					generation: 4, observedAt: now, state: native.PluginLoaded, retryAt: now.Add(time.Minute),
				}
			}
			cut, target := controller.nextPlugin(now)
			require.Nil(t, cut)
			require.Nil(t, target)
			require.NoError(t, controller.update(view))
			observed := controller.observation(4, selected.Digest, now)
			require.Equal(t, native.PluginLoaded, observed.state)
			require.Equal(t, now, observed.observedAt)
			stale, err := parsePluginIntent(pluginSnapshot())
			require.NoError(t, err)
			require.NoError(t, controller.update(stale))
			require.Same(t, view, controller.view)
			changed := common.CloneProto(snapshot)
			changed.DesiredLoads[selected.Id] = old.Digest
			conflict, err := parsePluginIntent(changed)
			require.NoError(t, err)
			require.ErrorContains(t, controller.update(conflict), "same generation")
			require.Same(t, view, controller.view)
			changed.Generation++
			next, err := parsePluginIntent(changed)
			require.NoError(t, err)
			require.NoError(t, controller.update(next))
			require.Empty(t, controller.observation(5, old.Digest, now).state)
			cut, target = controller.nextPlugin(now)
			require.Same(t, next, cut)
			require.Equal(t, old.Digest, target.Digest)
		})
	}
}

func TestRuntimePluginObservationFreshness(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	for _, test := range []struct {
		name       string
		generation uint64
		time       time.Time
		valid      bool
	}{
		{name: "fresh", generation: 2, time: now.Add(-time.Minute), valid: true},
		{name: "stale", generation: 2, time: now.Add(-3 * time.Minute)},
		{name: "future", generation: 2, time: now.Add(3 * time.Second)},
		{name: "no completed probe", generation: 2},
		{name: "old cut", generation: 1, time: now},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			view, err := parsePluginIntent(pluginSnapshot())
			require.NoError(t, err)
			digest := view.snapshot.Plugins[0].Digest
			controller := &runtimePlugins{view: view, observations: map[string]runtimePluginObservation{
				digest: {generation: test.generation, observedAt: test.time, state: native.PluginLoaded},
			}}
			observed := controller.observation(2, digest, now)
			if test.valid {
				require.Equal(t, native.PluginLoaded, observed.state)
				require.Equal(t, test.time, observed.observedAt)
			} else {
				require.Empty(t, observed.state)
				require.Zero(t, observed.observedAt)
				require.NotEmpty(t, observed.problem)
			}
		})
	}
}

func TestReducePluginObservations(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	for _, test := range []struct {
		name     string
		load     bool
		states   []native.PluginHostState
		expected contractpb.PluginHostState
	}{
		{"every actual runtime loaded", true, []native.PluginHostState{native.PluginLoaded, native.PluginLoaded, native.PluginLoaded}, contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED},
		{"management alone cannot cover starting graph", true, []native.PluginHostState{native.PluginLoaded, ""}, contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED},
		{"new graph still installing", true, []native.PluginHostState{native.PluginLoaded, native.PluginInstalled}, contractpb.PluginHostState_PLUGIN_HOST_STATE_INSTALLED},
		{"management not loaded", true, []native.PluginHostState{native.PluginInstalled, native.PluginLoaded}, contractpb.PluginHostState_PLUGIN_HOST_STATE_INSTALLED},
		{"no load intent", false, []native.PluginHostState{native.PluginLoaded}, contractpb.PluginHostState_PLUGIN_HOST_STATE_INSTALLED},
		{"no actual namespace graphs", true, []native.PluginHostState{native.PluginLoaded}, contractpb.PluginHostState_PLUGIN_HOST_STATE_LOADED},
		{"failure on one graph", true, []native.PluginHostState{native.PluginLoaded, native.PluginFailed}, contractpb.PluginHostState_PLUGIN_HOST_STATE_FAILED},
		{"incompatible graph outranks failure", true, []native.PluginHostState{native.PluginFailed, native.PluginIncompatible}, contractpb.PluginHostState_PLUGIN_HOST_STATE_INCOMPATIBLE},
		{"restart requirement outranks unknown", true, []native.PluginHostState{"", native.PluginRestartRequired}, contractpb.PluginHostState_PLUGIN_HOST_STATE_RESTART_REQUIRED},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			samples := make([]runtimePluginObservation, len(test.states))
			for i, state := range test.states {
				samples[i] = runtimePluginObservation{
					state: state, observedAt: now.Add(-time.Duration(i) * time.Second),
				}
			}
			status := reducePluginObservations("host", test.load, samples)
			expected := &contractpb.PluginHostStatus{
				HostId: "host", State: test.expected,
				UpdatedAt: timestamppb.New(now.Add(-time.Duration(len(samples)-1) * time.Second)),
			}
			if test.expected == contractpb.PluginHostState_PLUGIN_HOST_STATE_UNSPECIFIED {
				expected.Message = "One or more local runtimes have no fresh plugin observation"
			}
			if test.expected == contractpb.PluginHostState_PLUGIN_HOST_STATE_INSTALLED && test.load {
				expected.Message = "Plugin loading is pending in one or more local runtimes"
			}
			protorequire.ProtoEqual(t, expected, status)
		})
	}
}
