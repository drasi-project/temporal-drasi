//go:build temporal_with_drasi && cgo

package native

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/await"
)

func workspace(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join(".verification", t.Name()+"-"+uuid.NewString()))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(root, 0o700))
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	return root
}

func configuration(path string) Config {
	return Config{
		StateDir: path, OwnerEpoch: 1, InstanceID: "go-native-test",
		Sources: []Source{{ID: "temporal", Kind: SourceTemporal}},
		Queries: []Query{{
			ID: "q", Revision: 1, Language: Cypher, Sources: []string{"temporal"},
			Text: "MATCH (n:Item) WHERE n.amount >= 10 RETURN n.name AS name, n.amount * 2 AS doubled",
		}},
	}
}

func openRuntime(t *testing.T, config Config) *Runtime {
	t.Helper()
	runtime, err := Open(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, runtime.Close(ctx))
	})
	return runtime
}

func feedCommand(position uint64, amount int) FeedCommand {
	return FeedCommand{
		SourceID: "temporal", Position: position,
		Change: Change{
			Kind: Insert, ElementID: "item-1", Labels: []string{"Item"}, EffectiveFrom: position,
			Properties: map[string]any{"name": "alpha", "amount": amount},
		},
	}
}

func requireSnapshot(t *testing.T, runtime *Runtime, expected string) Event {
	t.Helper()
	var snapshot Event
	await.Require(t.Context(), t, func(t *await.T) {
		result, err := runtime.Apply(t.Context(), SnapshotCommand{QueryID: "q"})
		require.NoError(t, err)
		require.NotNil(t, result.Snapshot)
		rows := make([]json.RawMessage, 0, len(result.Snapshot.Rows))
		for _, row := range result.Snapshot.Rows {
			rows = append(rows, row.Value)
		}
		encoded, err := json.Marshal(rows)
		require.NoError(t, err)
		require.JSONEq(t, expected, string(encoded))
		snapshot = *result.Snapshot
	}, 10*time.Second, 10*time.Millisecond)
	return snapshot
}

func TestNativeEvaluationAndPersistentHandoff(t *testing.T) {
	t.Parallel()
	require.True(t, Enabled())
	root := workspace(t)
	config := configuration(filepath.Join(root, "owner"))
	runtime := openRuntime(t, config)
	_, err := runtime.Apply(t.Context(), SubscribeCommand{Subscription: Subscription{
		ID: "watch", QueryID: "q", QueryRevision: 1,
	}})
	require.NoError(t, err)
	initial, err := runtime.Poll(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, initial, 1)
	require.Equal(t, EventSnapshot, initial[0].Kind)
	require.Empty(t, initial[0].Rows)
	require.NoError(t, runtime.Ack(t.Context(), initial[0].Receipt("initial-commit")))

	_, err = runtime.Apply(t.Context(), feedCommand(1, 12))
	require.NoError(t, err)
	requireSnapshot(t, runtime, `[{"name":"alpha","doubled":24}]`)
	pending, err := runtime.Poll(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, EventDelta, pending[0].Kind)
	require.Len(t, pending[0].Changes, 1)
	require.Equal(t, "ADD", pending[0].Changes[0].Type)
	require.JSONEq(t, `{"name":"alpha","doubled":24}`, string(pending[0].Changes[0].Data))
	repeated, err := runtime.Poll(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, pending, repeated)
	require.NoError(t, runtime.Close(t.Context()))
	require.NoError(t, runtime.Close(t.Context()))

	restarted := openRuntime(t, config)
	requireSnapshot(t, restarted, `[{"name":"alpha","doubled":24}]`)
	replayed, err := restarted.Poll(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, pending, replayed)
	skipped := pending[0].Receipt("invalid-commit")
	skipped.Sequence++
	err = restarted.Ack(t.Context(), skipped)
	nativeError, ok := errors.AsType[*Error](err)
	require.True(t, ok)
	require.Equal(t, CodeGap, nativeError.Code)
	accepted := pending[0].Receipt("durable-temporal-commit")
	require.NoError(t, restarted.Ack(t.Context(), accepted))
	require.NoError(t, restarted.Ack(t.Context(), accepted))
	_, err = restarted.Apply(t.Context(), feedCommand(1, 12))
	require.NoError(t, err)
	updated := feedCommand(2, 17)
	updated.Change.Kind = Update
	_, err = restarted.Apply(t.Context(), updated)
	require.NoError(t, err)
	requireSnapshot(t, restarted, `[{"name":"alpha","doubled":34}]`)
	update, err := restarted.Poll(t.Context(), 1)
	require.NoError(t, err)
	require.Equal(t, pending[0].Sequence+1, update[0].Sequence)
	require.Equal(t, "UPDATE", update[0].Changes[0].Type)
}

func TestNativeCompletePropertyValuesReplaceWithoutRowReentry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		properties map[string]any
		expected   string
	}{
		{
			name: "nested property removal",
			properties: map[string]any{
				"details":  map[string]any{"attributes": map[string]any{"retained": true}},
				"optional": "present",
			},
			expected: `[{"details":{"attributes":{"retained":true}},"optional":"present"}]`,
		},
		{
			name: "nullable property clearing",
			properties: map[string]any{
				"details":  map[string]any{"attributes": map[string]any{"removed": "old", "retained": true}},
				"optional": nil,
			},
			expected: `[{"details":{"attributes":{"removed":"old","retained":true}},"optional":null}]`,
		},
		{
			name: "empty nested map and cleared property",
			properties: map[string]any{
				"details":  map[string]any{"attributes": map[string]any{}},
				"optional": nil,
			},
			expected: `[{"details":{"attributes":{}},"optional":null}]`,
		},
		{
			name: "typed array replacement",
			properties: map[string]any{
				"details":  []any{"retained", 3, true, nil, map[string]any{"nested": []string{"value"}}},
				"optional": nil,
			},
			expected: `[{"details":["retained",3,true,null,{"nested":["value"]}],"optional":null}]`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := configuration(filepath.Join(workspace(t), "owner"))
			config.Queries[0].Text = "MATCH (n:Item) RETURN n.details AS details, n.optional AS optional"
			runtime := openRuntime(t, config)
			before := feedCommand(1, 0)
			before.Change.Properties = map[string]any{
				"details":  map[string]any{"attributes": map[string]any{"removed": "old", "retained": true}},
				"optional": "present",
			}
			_, err := runtime.Apply(t.Context(), before)
			require.NoError(t, err)
			initial := requireSnapshot(t, runtime,
				`[{"details":{"attributes":{"removed":"old","retained":true}},"optional":"present"}]`)
			_, err = runtime.Apply(t.Context(), SubscribeCommand{Subscription: Subscription{
				ID: "watch", QueryID: "q", QueryRevision: 1,
			}})
			require.NoError(t, err)
			boundary, err := runtime.Poll(t.Context(), 1)
			require.NoError(t, err)
			require.Len(t, boundary, 1)
			require.NoError(t, runtime.Ack(t.Context(), boundary[0].Receipt("accepted-initial")))

			after := feedCommand(2, 0)
			after.Change.Kind = Update
			after.Change.Properties = test.properties
			_, err = runtime.Apply(t.Context(), after)
			require.NoError(t, err)
			snapshot := requireSnapshot(t, runtime, test.expected)
			require.Equal(t, initial.Rows[0].Key, snapshot.Rows[0].Key)
			delta, err := runtime.Poll(t.Context(), 1)
			require.NoError(t, err)
			require.Len(t, delta, 1)
			require.Equal(t, initial.Sequence+1, delta[0].Sequence)
			require.Len(t, delta[0].Changes, 1)
			require.Equal(t, "UPDATE", delta[0].Changes[0].Type)
			require.JSONEq(t, string(initial.Rows[0].Value), string(delta[0].Changes[0].Before))
			require.JSONEq(t, string(snapshot.Rows[0].Value), string(delta[0].Changes[0].After))
			require.NoError(t, runtime.Close(t.Context()))
			reopened := openRuntime(t, config)
			require.Equal(t, snapshot, requireSnapshot(t, reopened, test.expected))
			replayed, err := reopened.Poll(t.Context(), 1)
			require.NoError(t, err)
			require.Equal(t, delta, replayed)
		})
	}
}

func TestNativeManagedCatalogLifecycle(t *testing.T) {
	t.Parallel()
	root := workspace(t)
	config := Config{
		StateDir: filepath.Join(root, "owner"), OwnerEpoch: 1,
		InstanceID: "managed-nodes", StartMode: StartModeDormant,
	}
	runtime := openRuntime(t, config)
	sourceDefinition := NodeDefinition{
		Lifetime: ResourceLifetime{
			NamespaceID: "00000000-0000-4000-8000-000000000001",
			ResourceID:  "source.events", Incarnation: "7",
		},
		Activation: 1, Recipe: TemporalSourceRecipe{},
	}
	source, err := runtime.Apply(t.Context(), AdmitNodeCommand{Definition: sourceDefinition})
	require.NoError(t, err)
	require.NotNil(t, source.Node)
	require.Equal(t, sourceDefinition, source.Node.Definition)
	seed := feedCommand(1, 12)
	seed.SourceID = source.Node.NativeID
	_, err = runtime.Apply(t.Context(), seed)
	require.NoError(t, err)
	queryDefinition := NodeDefinition{
		Lifetime: ResourceLifetime{
			NamespaceID: sourceDefinition.Lifetime.NamespaceID, ResourceID: "query.events", Incarnation: "8",
		},
		Activation: 1,
		Recipe: QueryRecipe{
			Language: Cypher, Text: "MATCH (n:Item) RETURN n.amount AS amount", Sources: []string{source.Node.NativeID},
		},
	}
	query, err := runtime.Apply(t.Context(), AdmitNodeCommand{Definition: queryDefinition})
	require.NoError(t, err)
	require.NotNil(t, query.Node)
	require.Equal(t, queryDefinition, query.Node.Definition)
	before, err := runtime.Apply(t.Context(), SnapshotCommand{QueryID: query.Node.NativeID})
	require.NoError(t, err)
	require.NotNil(t, before.Snapshot)
	require.Zero(t, before.Snapshot.Sequence)
	require.Len(t, before.Snapshot.Rows, 1)
	require.JSONEq(t, `{"amount":12}`, string(before.Snapshot.Rows[0].Value))
	_, err = runtime.Apply(t.Context(), StopNodeCommand{NativeID: source.Node.NativeID})
	require.ErrorContains(t, err, "stop dependent queries")
	for _, id := range []string{query.Node.NativeID, source.Node.NativeID} {
		_, err := runtime.Apply(t.Context(), StopNodeCommand{NativeID: id})
		require.NoError(t, err)
		await.Require(t.Context(), t, func(t *await.T) {
			observations, err := runtime.Apply(t.Context(), ObserveNodesCommand{})
			require.NoError(t, err)
			for _, node := range observations.Nodes {
				if node.NativeID == id {
					require.Equal(t, NodeStopped, node.Status)
					return
				}
			}
			t.Fatal("stopped node is missing from native observations")
		}, 10*time.Second, 10*time.Millisecond)
		_, err = runtime.Apply(t.Context(), StopNodeCommand{NativeID: id})
		require.NoError(t, err)
	}
	resumedSource, err := runtime.Apply(t.Context(), AdmitNodeCommand{Definition: sourceDefinition})
	require.NoError(t, err)
	require.Equal(t, source.Node, resumedSource.Node)
	resumedQuery, err := runtime.Apply(t.Context(), AdmitNodeCommand{Definition: queryDefinition})
	require.NoError(t, err)
	require.NotNil(t, resumedQuery.Node)
	require.Equal(t, query.Node.NativeID, resumedQuery.Node.NativeID)
	after, err := runtime.Apply(t.Context(), SnapshotCommand{QueryID: query.Node.NativeID})
	require.NoError(t, err)
	require.Equal(t, before.Snapshot, after.Snapshot)

	checkpointDir := filepath.Join(root, "checkpoint")
	_, err = runtime.Apply(t.Context(), CheckpointCommand{Destination: checkpointDir})
	require.NoError(t, err)
	require.NoError(t, runtime.Close(t.Context()))
	config.StateDir = filepath.Join(root, "successor")
	config.OwnerEpoch = 2
	config.RestoreFrom = checkpointDir
	successor := openRuntime(t, config)
	for _, definition := range []NodeDefinition{sourceDefinition, queryDefinition} {
		_, err := successor.Apply(t.Context(), AdmitNodeCommand{Definition: definition})
		require.NoError(t, err)
	}
	recovered, err := successor.Apply(t.Context(), SnapshotCommand{QueryID: query.Node.NativeID})
	require.NoError(t, err)
	require.Equal(t, before.Snapshot, recovered.Snapshot)
}

func TestNativeCheckpointRestoreToNewOwner(t *testing.T) {
	t.Parallel()
	root := workspace(t)
	config := configuration(filepath.Join(root, "owner-1"))
	runtime := openRuntime(t, config)
	_, err := runtime.Apply(t.Context(), feedCommand(1, 21))
	require.NoError(t, err)
	snapshot := requireSnapshot(t, runtime, `[{"name":"alpha","doubled":42}]`)
	checkpointDir := filepath.Join(root, "checkpoint")
	result, err := runtime.Apply(t.Context(), CheckpointCommand{Destination: checkpointDir})
	require.NoError(t, err)
	require.NotNil(t, result.Checkpoint)
	require.Contains(t, result.Checkpoint.Files, "state.redb")
	require.NoError(t, runtime.Close(t.Context()))

	config.StateDir = filepath.Join(root, "owner-2")
	config.OwnerEpoch = 2
	config.RestoreFrom = checkpointDir
	restored := openRuntime(t, config)
	recovered := requireSnapshot(t, restored, `[{"name":"alpha","doubled":42}]`)
	require.Equal(t, snapshot, recovered)
}

func TestNativeCancellationAndErrors(t *testing.T) {
	t.Parallel()
	runtime := openRuntime(t, configuration(filepath.Join(workspace(t), "owner")))
	_, err := runtime.Apply(t.Context(), nil)
	require.ErrorContains(t, err, "unknown or nil command")
	_, err = runtime.Apply(t.Context(), feedCommand(2, 12))
	nativeError, ok := errors.AsType[*Error](err)
	require.True(t, ok)
	require.Equal(t, CodeGap, nativeError.Code)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	_, err = runtime.Poll(ctx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, runtime.Close(t.Context()))
	_, err = runtime.Apply(t.Context(), DescriptorsCommand{})
	require.ErrorIs(t, err, ErrClosed)
}

func TestNativeCatalogOperationsAreHostLocalAndFailClosed(t *testing.T) {
	t.Parallel()
	config := configuration(filepath.Join(workspace(t), "owner"))
	config.Catalogs = []CatalogConfig{{
		ID: "trusted", DisplayName: "Trusted plugins",
		Plugins: []CatalogRelease{{
			Version: "1.0.0",
			Artifact: Plugin{
				ID: "source", Reference: "registry.example/trusted/source/test@sha256:" + strings.Repeat("a", 64),
				BinarySHA256: strings.Repeat("b", 64), Filename: "libdrasi_test.so",
			},
		}},
	}}
	runtime := openRuntime(t, config)
	catalogs, err := runtime.Apply(t.Context(), ListCatalogsCommand{})
	require.NoError(t, err)
	require.Equal(t, []Catalog{{ID: "trusted", DisplayName: "Trusted plugins"}}, catalogs.Catalogs)
	plugins, err := runtime.Apply(t.Context(), DiscoverPluginsCommand{CatalogID: "trusted", Search: "no-match"})
	require.NoError(t, err)
	require.Empty(t, plugins.DiscoveredPlugins)
	_, err = runtime.Apply(t.Context(), DiscoverPluginsCommand{CatalogID: "attacker.example/plugins"})
	require.ErrorContains(t, err, "catalog is not configured")
	selection := PluginSelection{CatalogID: "trusted", PluginID: "source", Version: "1.0.0"}
	_, err = runtime.Apply(t.Context(), InstallPluginCommand{Selection: selection, Digest: "sha256:wrong"})
	require.ErrorContains(t, err, "differs from the configured catalog")
	digest := "sha256:" + strings.Repeat("a", 64)
	_, err = runtime.Apply(t.Context(), LoadPluginCommand{Digest: digest})
	require.ErrorContains(t, err, "not installed")
	installation, err := runtime.Apply(t.Context(), GetPluginInstallationCommand{Digest: digest})
	require.NoError(t, err)
	require.NotNil(t, installation.Installation)
	require.Equal(t, PluginFailed, installation.Installation.State)
	require.Nil(t, installation.Installation.VerifiedSignerIdentity)
	all, err := runtime.Apply(t.Context(), ListPluginInstallationsCommand{})
	require.NoError(t, err)
	require.Len(t, all.Installations, 1)
	require.Equal(t, installation.Installation, &all.Installations[0])
	descriptors, err := runtime.Descriptors(t.Context())
	require.NoError(t, err)
	require.Empty(t, descriptors.Descriptors)
	_, err = runtime.Apply(t.Context(), feedCommand(1, 12))
	require.NoError(t, err)
	requireSnapshot(t, runtime, `[{"name":"alpha","doubled":24}]`)
}

func blockedPluginCatalog(t *testing.T) (CatalogConfig, <-chan struct{}, func()) {
	t.Helper()
	started := make(chan struct{}, 8)
	release := make(chan struct{})
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
		return nil, errors.New("plugin metadata fixture released")
	}}
	server.StartTLS()
	t.Cleanup(server.Close)
	registry := strings.TrimPrefix(server.URL, "https://") + "/trusted"
	catalog := CatalogConfig{
		ID: "trusted", DisplayName: "Trusted plugins",
		Plugins: []CatalogRelease{{
			Version: "1.0.0",
			Artifact: Plugin{
				ID: "source", Reference: registry + "/source@sha256:" + strings.Repeat("a", 64),
				BinarySHA256: strings.Repeat("b", 64), Filename: "libdrasi_test.so",
			},
		}},
	}
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	return catalog, started, unblock
}

func TestNativePendingOutputRemainsServiceableDuringPluginIO(t *testing.T) {
	t.Parallel()
	catalog, started, release := blockedPluginCatalog(t)
	config := configuration(filepath.Join(workspace(t), "owner"))
	config.Catalogs = []CatalogConfig{catalog}
	runtime := openRuntime(t, config)
	defer release()
	_, err := runtime.Apply(t.Context(), SubscribeCommand{Subscription: Subscription{
		ID: "watch", QueryID: "q", QueryRevision: 1,
	}})
	require.NoError(t, err)
	pending, err := runtime.Poll(t.Context(), 1)
	require.NoError(t, err)
	type inspected struct {
		result Result
		err    error
	}
	completed := make(chan inspected, 1)
	provisioning, cancelProvisioning := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancelProvisioning()
	go func() {
		result, err := runtime.Apply(provisioning, InspectPluginCommand{
			Selection: PluginSelection{CatalogID: "trusted", PluginID: "source", Version: "1.0.0"},
		})
		completed <- inspected{result, err}
	}()
	await.Rcv(t, started)

	output, cancelOutput := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelOutput()
	replayed, err := runtime.Poll(output, 1)
	require.NoError(t, err)
	require.Equal(t, pending, replayed)
	require.NoError(t, runtime.Ack(output, pending[0].Receipt("accepted-during-provisioning")))
	_, err = runtime.Apply(output, feedCommand(1, 12))
	require.NoError(t, err)
	requireSnapshot(t, runtime, `[{"name":"alpha","doubled":24}]`)
	next, err := runtime.Poll(output, 1)
	require.NoError(t, err)
	require.Equal(t, pending[0].Sequence+1, next[0].Sequence)
	require.Equal(t, pending[0].Incarnation, next[0].Incarnation)
	require.NoError(t, runtime.Ack(output, next[0].Receipt("next-accepted-during-provisioning")))
	select {
	case result := <-completed:
		t.Fatalf("plugin I/O completed before its fixture was released: %+v", result)
	default:
	}
	cancelProvisioning()
	require.ErrorIs(t, await.Rcv(t, completed).err, context.Canceled)
	updated := feedCommand(2, 17)
	updated.Change.Kind = Update
	_, err = runtime.Apply(output, updated)
	require.NoError(t, err)
	requireSnapshot(t, runtime, `[{"name":"alpha","doubled":34}]`)
	afterCancellation, err := runtime.Poll(output, 1)
	require.NoError(t, err)
	require.Equal(t, next[0].Sequence+1, afterCancellation[0].Sequence)
	require.Equal(t, next[0].Incarnation, afterCancellation[0].Incarnation)
	require.NoError(t, runtime.Ack(output, afterCancellation[0].Receipt("accepted-after-provisioning-cancelled")))
}

func TestNativeConcurrentFeedPositionsRemainImmutable(t *testing.T) {
	t.Parallel()
	runtime := openRuntime(t, configuration(filepath.Join(workspace(t), "owner")))
	start := make(chan struct{})
	type fed struct {
		amount int
		err    error
	}
	completed := make(chan fed, 2)
	for _, amount := range []int{12, 17} {
		go func() {
			<-start
			_, err := runtime.Apply(t.Context(), feedCommand(1, amount))
			completed <- fed{amount, err}
		}()
	}
	close(start)
	first, second := await.Rcv(t, completed), await.Rcv(t, completed)
	if first.err != nil {
		first, second = second, first
	}
	require.NoError(t, first.err)
	nativeError, ok := errors.AsType[*Error](second.err)
	require.True(t, ok)
	require.Equal(t, CodeConflict, nativeError.Code)
	expected, err := json.Marshal([]map[string]any{{"name": "alpha", "doubled": first.amount * 2}})
	require.NoError(t, err)
	requireSnapshot(t, runtime, string(expected))
	_, err = runtime.Apply(t.Context(), feedCommand(1, first.amount))
	require.NoError(t, err)
	_, err = runtime.Apply(t.Context(), feedCommand(1, second.amount))
	require.ErrorContains(t, err, "already used for a different change")
}
