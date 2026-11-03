//go:build temporal_with_drasi && cgo

package native

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func TestPluginCompletionRejectsInvalidCommandsWithoutNativeWork(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		command Command
		message string
	}{
		{"nil", nil, "unknown or nil"},
		{"typed nil", (*LoadPluginCommand)(nil), "unknown or nil"},
		{"feed", FeedCommand{}, "requires a management command"},
		{"checkpoint", CheckpointCommand{}, "requires a management command"},
		{"oversized", DiscoverPluginsCommand{Search: strings.Repeat("x", 1<<20)}, "byte limit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var runtime Runtime
			_, err := runtime.Plugin(test.command)
			require.ErrorContains(t, err, test.message)
		})
	}
}

func TestPluginCompletionAttemptReplayAndFencing(t *testing.T) {
	t.Parallel()
	runtime := openRuntime(t, configuration(filepath.Join(workspace(t), "owner")))
	var invalid PluginAttempt
	_, err := invalid.Wait(t.Context())
	require.ErrorContains(t, err, "has no runtime")

	stale, err := runtime.Plugin(ListCatalogsCommand{})
	require.NoError(t, err)
	current, err := runtime.Plugin(ListCatalogsCommand{})
	require.NoError(t, err)
	expected, err := current.Wait(t.Context())
	require.NoError(t, err)
	_, err = stale.Wait(t.Context())
	require.ErrorContains(t, err, "stale")

	canceled, err := runtime.Plugin(ListCatalogsCommand{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = canceled.Wait(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = canceled.Wait(t.Context())
	require.NoError(t, err)

	failed, err := runtime.Plugin(LoadPluginCommand{Digest: "unknown"})
	require.NoError(t, err)
	_, err = failed.Wait(t.Context())
	require.ErrorContains(t, err, "not in a configured catalog")
	nativeError, ok := errors.AsType[*Error](err)
	require.True(t, ok)
	nativeError.Message = "caller mutation"
	_, err = failed.Wait(t.Context())
	require.ErrorContains(t, err, "not in a configured catalog")

	runtime.mu.Lock()
	runtime.nextPlugin = math.MaxUint64
	runtime.mu.Unlock()
	_, err = runtime.Plugin(ListCatalogsCommand{})
	require.ErrorContains(t, err, "sequence exhausted")

	require.NoError(t, runtime.Close(t.Context()))
	replayed, err := current.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, expected, replayed)
	_, err = runtime.Plugin(ListCatalogsCommand{})
	require.ErrorIs(t, err, ErrClosed)
}

func TestPluginCompletionFreezesCommandsAndDetachesResults(t *testing.T) {
	t.Parallel()
	catalog, trust, _, release := blockedPluginCatalog(t)
	defer release()
	config := configuration(filepath.Join(workspace(t), "owner"))
	config.Catalogs, config.Trust = []CatalogConfig{catalog}, trust
	runtime := openRuntime(t, config)
	command := &DiscoverPluginsCommand{CatalogID: "trusted", Search: "excluded"}
	attempt, err := runtime.Plugin(command)
	require.NoError(t, err)
	command.CatalogID = "unconfigured"
	result, err := attempt.Wait(t.Context())
	require.NoError(t, err)
	require.Empty(t, result.DiscoveredPlugins)

	catalogs, err := runtime.Plugin(ListCatalogsCommand{})
	require.NoError(t, err)
	first, err := catalogs.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, []Catalog{{ID: catalog.ID, DisplayName: catalog.DisplayName}}, first.Catalogs)
	first.Catalogs[0].ID = "caller mutation"
	second, err := catalogs.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, []Catalog{{ID: catalog.ID, DisplayName: catalog.DisplayName}}, second.Catalogs)
}

func TestPluginCompletionJoinsCanceledWaitBeforeClose(t *testing.T) {
	t.Parallel()
	catalog, trust, started, release := blockedPluginCatalog(t)
	defer release()
	config := configuration(filepath.Join(workspace(t), "owner"))
	config.Catalogs, config.Trust = []CatalogConfig{catalog}, trust
	runtime := openRuntime(t, config)
	attempt, err := runtime.Plugin(InspectPluginCommand{
		Selection: PluginSelection{CatalogID: "trusted", PluginID: "source", Version: "1.0.0"},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	completed := make(chan error, 1)
	go func() {
		_, err := attempt.Wait(ctx)
		completed <- err
	}()
	await.Rcv(t, started)
	cancel()
	require.ErrorIs(t, await.Rcv(t, completed), context.Canceled)

	_, err = runtime.Apply(t.Context(), ListCatalogsCommand{})
	require.ErrorContains(t, err, "still in progress")
	closing, cancelClosing := context.WithCancel(t.Context())
	defer cancelClosing()
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close(closing) }()
	await.RequireTrue(t, func() bool {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.pluginClosing
	}, 5*time.Second, time.Millisecond)
	cancelClosing()
	require.ErrorIs(t, await.Rcv(t, closed), context.Canceled)
	_, err = runtime.runtimeHandle()
	require.NoError(t, err)
	_, err = runtime.Plugin(ListCatalogsCommand{})
	require.ErrorContains(t, err, "sealed")

	release()
	result, err := attempt.Wait(t.Context())
	require.NoError(t, err)
	require.NotNil(t, result.Plugin)
	require.False(t, result.Plugin.Compatible)
	require.NoError(t, runtime.Close(t.Context()))
}

//parallelize:ignore
func TestPluginCompletionProcessCapacitySurvivesCanceledWaiters(t *testing.T) {
	catalog, trust, started, release := blockedPluginCatalog(t)
	defer release()
	root := workspace(t)
	var attempts []*PluginAttempt
	var cancelWaiters []context.CancelFunc
	completed := make(chan error, 8)
	for range 8 {
		runtime := openRuntime(t, Config{
			StateDir: filepath.Join(root, "owner-"+uuid.NewString()), OwnerEpoch: 1, InstanceID: "capacity",
			StartMode: StartModeDormant, Catalogs: []CatalogConfig{catalog}, Trust: trust,
		})
		attempt, err := runtime.Plugin(InspectPluginCommand{
			Selection: PluginSelection{CatalogID: "trusted", PluginID: "source", Version: "1.0.0"},
		})
		require.NoError(t, err)
		attempts = append(attempts, attempt)
		ctx, cancel := context.WithCancel(t.Context())
		cancelWaiters = append(cancelWaiters, cancel)
		t.Cleanup(cancel)
		go func() {
			_, err := attempt.Wait(ctx)
			completed <- err
		}()
	}
	for range 8 {
		await.Rcv(t, started)
	}
	for _, cancel := range cancelWaiters {
		cancel()
	}
	for range 8 {
		require.ErrorIs(t, await.Rcv(t, completed), context.Canceled)
	}
	overflow := openRuntime(t, Config{
		StateDir: filepath.Join(root, "overflow"), OwnerEpoch: 1, InstanceID: "overflow",
		StartMode: StartModeDormant, Catalogs: []CatalogConfig{catalog}, Trust: trust,
	})
	retry, err := overflow.Plugin(InspectPluginCommand{
		Selection: PluginSelection{CatalogID: "trusted", PluginID: "source", Version: "1.0.0"},
	})
	require.NoError(t, err)
	_, err = retry.Wait(t.Context())
	require.ErrorContains(t, err, "capacity exhausted")
	handle, err := overflow.runtimeHandle()
	require.NoError(t, err)
	payload, err := marshalCommand(InspectPluginCommand{
		Selection: PluginSelection{CatalogID: "trusted", PluginID: "source", Version: "1.0.0"},
	})
	require.NoError(t, err)
	_, err = callPluginJoined(uint64(handle), payload)
	require.ErrorContains(t, err, "native plugin work capacity exhausted")

	release()
	for _, attempt := range attempts {
		result, err := attempt.Wait(t.Context())
		require.NoError(t, err)
		require.NotNil(t, result.Plugin)
		require.False(t, result.Plugin.Compatible)
	}
	result, err := retry.Wait(t.Context())
	require.NoError(t, err)
	require.NotNil(t, result.Plugin)
	require.False(t, result.Plugin.Compatible)
}
