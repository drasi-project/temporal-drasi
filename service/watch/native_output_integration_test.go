//go:build temporal_with_drasi && cgo

package watch

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	contractpb "github.com/drasi-project/temporal-drasi/contracts/watch/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestDecodedNativeChangesMatchCurrentResults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, query string }{
		{"projected rows", "MATCH (n:Item) RETURN n.id AS id, n.amount AS amount"},
		{"grouped aggregate", "MATCH (n:Item) RETURN n.group AS category, sum(n.amount) AS total"},
		{"filtered aggregate", "MATCH (n:Item) WITH n.group AS category, sum(n.amount) AS total WHERE total > 0 RETURN category, total"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := testcontext.For(t)
			config := native.Config{
				StateDir: filepath.Join(t.TempDir(), "runtime"), OwnerEpoch: 1, InstanceID: "decode-integration",
				Sources: []native.Source{{ID: "feed", Kind: native.SourceTemporal}},
				Queries: []native.Query{{ID: "query", Revision: 1, Language: native.Cypher, Sources: []string{"feed"}, Text: tc.query}},
			}
			runtime, err := native.Open(ctx, config)
			require.NoError(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				require.NoError(t, runtime.Close(ctx))
			})
			_, err = runtime.Apply(ctx, native.SubscribeCommand{Subscription: native.Subscription{
				ID: "watch", QueryID: "query", QueryRevision: 1,
			}})
			require.NoError(t, err)
			events, err := runtime.Poll(ctx, 1)
			require.NoError(t, err)
			require.Len(t, events, 1)
			initial, err := DecodeNativeOutput(events[0])
			require.NoError(t, err)
			rows := make(map[string]*structpb.Struct)
			for _, row := range initial.Changes {
				rows[row.Identity] = row.After
			}
			require.NoError(t, runtime.Ack(ctx, events[0].Receipt("initial")))
			for position, kind := range []native.ChangeKind{native.Insert, native.Update, native.Delete} {
				_, err := runtime.Apply(ctx, native.FeedCommand{
					SourceID: "feed", Position: uint64(position + 1),
					Change: native.Change{
						Kind: kind, ElementID: "item", Labels: []string{"Item"}, EffectiveFrom: uint64(position + 1),
						Properties: map[string]any{"id": "item", "group": "group", "amount": 12 + position},
					},
				})
				require.NoError(t, err)
				events, err := runtime.Poll(ctx, 1)
				require.NoError(t, err)
				require.Len(t, events, 1)
				pending := events
				require.NoError(t, runtime.Close(ctx))
				runtime, err = native.Open(ctx, config)
				require.NoError(t, err)
				events, err = runtime.Poll(ctx, 1)
				require.NoError(t, err)
				require.Equal(t, pending, events)
				output, err := DecodeNativeOutput(events[0])
				require.NoError(t, err, "native changes: %+v", events[0].Changes)
				require.NotEmpty(t, output.Changes)
				for _, row := range output.Changes {
					require.NoError(t, row.validate(false))
					switch row.Trigger {
					case contractpb.Trigger_TRIGGER_ENTER:
						_, exists := rows[row.Identity]
						require.False(t, exists)
						rows[row.Identity] = row.After
					case contractpb.Trigger_TRIGGER_UPDATE:
						require.Empty(t, cmp.Diff(rows[row.Identity], row.Before, protocmp.Transform()))
						rows[row.Identity] = row.After
					case contractpb.Trigger_TRIGGER_LEAVE:
						require.Empty(t, cmp.Diff(rows[row.Identity], row.Before, protocmp.Transform()))
						delete(rows, row.Identity)
					default:
						t.Fatalf("unexpected decoded trigger %v", row.Trigger)
					}
				}
				current, err := runtime.Apply(ctx, native.SnapshotCommand{QueryID: "query"})
				require.NoError(t, err)
				require.NotNil(t, current.Snapshot)
				snapshot, err := DecodeNativeOutput(*current.Snapshot)
				require.NoError(t, err)
				expected := make(map[string]*structpb.Struct)
				for _, row := range snapshot.Changes {
					expected[row.Identity] = row.After
				}
				require.Empty(t, cmp.Diff(expected, rows, protocmp.Transform()))
				require.NoError(t, runtime.Ack(ctx, events[0].Receipt("committed-"+string(kind))))
			}
		})
	}
}
