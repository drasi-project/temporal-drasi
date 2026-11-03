//go:build temporal_with_drasi && cgo

package watch

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/watch/native"
)

func TestHistoryInputNativeRelationshipsAndWarmRecovery(t *testing.T) {
	t.Parallel()
	ctx := testcontext.For(t)
	record := historyTestRecord()
	record.State.Workflow.SearchAttributes = map[string]tasks.FeedSearchAttribute{
		"LargeInteger": {Type: enumspb.INDEXED_VALUE_TYPE_INT, Value: json.RawMessage("9007199254740993")},
	}
	record.State.Activities = []tasks.FeedActivity{{ID: "activity", ScheduledEventID: 3, Attempt: 2, Status: "STARTED"}}
	payload, err := json.Marshal(record)
	require.NoError(t, err)
	runtime, err := native.Open(ctx, native.Config{
		InstanceID: record.NamespaceID, OwnerEpoch: 1, StartMode: native.StartModeDormant,
		StateDir: filepath.Join(t.TempDir(), "native"),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, runtime.Close(shutdown))
	})
	scope := native.HistoryScope{NamespaceID: record.NamespaceID, ShardCount: 1}
	sourceDefinition := native.NodeDefinition{
		Lifetime:   native.ResourceLifetime{NamespaceID: record.NamespaceID, ResourceID: "source", Incarnation: "1"},
		Activation: 1, Recipe: native.HistorySourceRecipe{Scope: scope},
	}
	source, err := runtime.Apply(ctx, native.AdmitNodeCommand{Definition: sourceDefinition})
	require.NoError(t, err)
	require.Equal(t, native.NodeStarting, source.Node.Status)
	worker := historyInputWorker{
		runtime: runtime, source: source.Node.NativeID, scope: scope,
		client: historyFeedTestClient{
			bootstrap: func(context.Context, *historyservice.BootstrapExecutionFeedRequest) (*historyservice.BootstrapExecutionFeedResponse, error) {
				return &historyservice.BootstrapExecutionFeedResponse{CaptureEpoch: 7, ReconstructionStart: 10, BootstrapCutoff: 12}, nil
			},
			read: func(_ context.Context, request *historyservice.GetExecutionFeedRequest) (*historyservice.GetExecutionFeedResponse, error) {
				response := &historyservice.GetExecutionFeedResponse{NextPosition: request.StartPosition, PublishedCutoff: 12}
				if request.EndPosition > request.StartPosition {
					response.NextPosition = 12
					if request.StartPosition <= 10 {
						response.Records = [][]byte{payload}
					}
				}
				return response, nil
			},
		},
	}
	ready, err := worker.advance(ctx, 1)
	require.NoError(t, err)
	require.False(t, ready)
	ready, err = worker.advance(ctx, 1)
	require.NoError(t, err)
	require.True(t, ready)
	queryDefinition := native.NodeDefinition{
		Lifetime:   native.ResourceLifetime{NamespaceID: record.NamespaceID, ResourceID: "query", Incarnation: "1"},
		Activation: 1, Recipe: native.QueryRecipe{
			Language: native.Cypher, Sources: []string{source.Node.NativeID},
			Text: "MATCH (w:Workflow)-[:HAS_ACTIVITY]->(a:Activity) RETURN w.workflow_id AS workflow, a.data.attempt AS attempt, w.data.search_attributes.LargeInteger.value AS large",
		},
	}
	query, err := runtime.Apply(ctx, native.AdmitNodeCommand{Definition: queryDefinition})
	require.NoError(t, err)
	snapshot, err := runtime.Apply(ctx, native.SnapshotCommand{QueryID: query.Node.NativeID})
	require.NoError(t, err)
	require.Len(t, snapshot.Snapshot.Rows, 1)
	require.JSONEq(t, `{"workflow":"workflow","attempt":2,"large":9007199254740993}`, string(snapshot.Snapshot.Rows[0].Value))
	for _, id := range []string{query.Node.NativeID, source.Node.NativeID} {
		_, err = runtime.Apply(ctx, native.StopNodeCommand{NativeID: id})
		require.NoError(t, err)
	}
	_, err = runtime.Apply(ctx, native.AdmitNodeCommand{Definition: sourceDefinition})
	require.NoError(t, err)
	_, err = runtime.Apply(ctx, native.SnapshotCommand{QueryID: query.Node.NativeID})
	require.ErrorContains(t, err, "not ready")
	ready, err = worker.advance(ctx, 1)
	require.NoError(t, err)
	require.True(t, ready)
	_, err = runtime.Apply(ctx, native.AdmitNodeCommand{Definition: queryDefinition})
	require.NoError(t, err)
	await.Require(ctx, t, func(t *await.T) {
		after, err := runtime.Apply(t.Context(), native.SnapshotCommand{QueryID: query.Node.NativeID})
		require.NoError(t, err)
		require.Equal(t, snapshot.Snapshot, after.Snapshot)
	}, 5*time.Second, 10*time.Millisecond)
	_, err = runtime.Apply(ctx, native.SuspendHistoryCommand{SourceID: source.Node.NativeID})
	require.NoError(t, err)
	_, err = runtime.Apply(ctx, native.SnapshotCommand{QueryID: query.Node.NativeID})
	require.ErrorContains(t, err, "not ready")
}
