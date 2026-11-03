//go:build temporal_with_drasi && cgo && (darwin || linux)

package watch

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/watchassignment"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/protobuf/proto"
)

type graphCrashReady struct {
	SourceID       string        `json:"source_id"`
	QueryID        string        `json:"query_id"`
	SubscriptionID string        `json:"subscription_id"`
	Pending        *native.Event `json:"pending"`
}

func TestGraphNativeOwnerProcess(t *testing.T) {
	t.Parallel()
	root := os.Getenv("TEMPORAL_DRASI_CRASH_ROOT")
	if root == "" {
		t.Skip("subprocess fixture")
	}
	ready := os.NewFile(3, "drasi-crash-ready")
	require.NotNil(t, ready)
	client := &graphAssignmentClient{
		describe: func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
			return nil, serviceerror.NewNotFound("no assignment")
		},
		begin: func(_ context.Context, request *assignmentpb.BeginRequest) (*assignmentpb.State, error) {
			return graphActiveState(t, request), nil
		},
	}
	options := graphCrashOptions(root, client)
	graph, err := OpenGraph(t.Context(), options)
	require.NoError(t, err)
	source, err := graph.runtime.Apply(t.Context(), graphCrashSource(options.NamespaceID))
	require.NoError(t, err)
	require.NotNil(t, source.Node)
	query, err := graph.runtime.Apply(t.Context(), graphCrashQuery(options.NamespaceID, source.Node.NativeID))
	require.NoError(t, err)
	require.NotNil(t, query.Node)
	_, err = graph.runtime.Apply(t.Context(), graphCrashFeed(source.Node.NativeID, 1, "before-crash"))
	require.NoError(t, err)
	await.Require(t.Context(), t, func(t *await.T) {
		result, err := graph.runtime.Apply(t.Context(), native.SnapshotCommand{QueryID: query.Node.NativeID})
		require.NoError(t, err)
		require.NotNil(t, result.Snapshot)
		require.Len(t, result.Snapshot.Rows, 1)
	}, 5*time.Second, 10*time.Millisecond)
	bound, err := graph.runtime.Apply(t.Context(), native.BindWatchCommand{
		QueryID: query.Node.NativeID,
		Binding: native.WatchBinding{
			Watch:         native.ResourceLifetime{NamespaceID: options.NamespaceID, ResourceID: "crash.watch", Incarnation: "23"},
			WatchRevision: 23, Query: graphCrashQuery(options.NamespaceID, source.Node.NativeID).Definition.Lifetime, QueryRevision: 11,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, bound.Subscription)
	pending, err := graph.runtime.Apply(t.Context(), native.PollWatchCommand{SubscriptionID: bound.Subscription.Definition.ID})
	require.NoError(t, err)
	require.NotNil(t, pending.Event)
	require.NoError(t, json.NewEncoder(ready).Encode(graphCrashReady{
		SourceID: source.Node.NativeID, QueryID: query.Node.NativeID,
		SubscriptionID: bound.Subscription.Definition.ID, Pending: pending.Event,
	}))
	require.NoError(t, ready.Close())
	<-t.Context().Done()
}

func TestGraphNativeProcessCrashRetainsExactPendingOutput(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"ordinary recovery", "lost committed seal reply", "retry uncommitted seal"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			var state *assignmentpb.State
			var submissions []*assignmentpb.SealRequest
			client := &graphAssignmentClient{
				describe: func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
					return common.CloneProto(state), nil
				},
				begin: func(_ context.Context, request *assignmentpb.BeginRequest) (*assignmentpb.State, error) {
					if state.GetActive() != nil {
						return nil, serviceerror.NewFailedPrecondition("an owner remains active")
					}
					require.Equal(t, state.Version, request.ExpectedVersion)
					sealed := state.GetSealed()
					state = graphActiveState(t, request)
					state.GetActive().Grant.Epoch = sealed.Predecessor.Grant.Epoch + 1
					state.GetActive().RecoveryCut = common.CloneProto(sealed.Checkpoint)
					return common.CloneProto(state), nil
				},
				seal: func(_ context.Context, request *assignmentpb.SealRequest) (*assignmentpb.State, error) {
					submissions = append(submissions, common.CloneProto(request))
					if len(submissions) == 1 && failure == "retry uncommitted seal" {
						return nil, serviceerror.NewUnavailable("recovery seal did not commit")
					}
					active := state.GetActive()
					if active == nil || request.ExpectedVersion != state.Version || !proto.Equal(active.Grant, request.Grant) {
						return nil, serviceerror.NewFailedPrecondition("seal is fenced")
					}
					state = graphSealedState(t, active, request)
					if len(submissions) == 1 && failure == "lost committed seal reply" {
						return nil, serviceerror.NewUnavailable("recovery seal reply lost after commit")
					}
					return common.CloneProto(state), nil
				},
			}
			options := graphCrashOptions(t.TempDir(), client)
			state = graphActiveState(t, &assignmentpb.BeginRequest{
				NamespaceId: options.NamespaceID, ExecutionId: watchassignment.BusinessID, StorageHostId: options.StorageHostID,
				ProcessBootId: options.ProcessBootID, RequestId: options.RequestID,
			})
			reader, writer, err := os.Pipe()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reader.Close()) })
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestGraphNativeOwnerProcess$")
			command.Env = append(os.Environ(), "TEMPORAL_DRASI_CRASH_ROOT="+options.StateRoot, "RUST_LOG=error")
			command.ExtraFiles = []*os.File{writer}
			outputPath := filepath.Join(t.TempDir(), "owner.log")
			output, err := os.Create(outputPath)
			require.NoError(t, err)
			command.Stdout, command.Stderr = output, output
			require.NoError(t, command.Start())
			require.NoError(t, writer.Close())
			exited := make(chan error, 1)
			go func() { exited <- command.Wait() }()
			waited := false
			t.Cleanup(func() {
				cancel()
				if !waited {
					<-exited
				}
				require.NoError(t, output.Close())
				if t.Failed() {
					log, err := os.ReadFile(outputPath)
					require.NoError(t, err)
					t.Log(string(log))
				}
			})
			type decodedReady struct {
				ready graphCrashReady
				err   error
			}
			ready := make(chan decodedReady, 1)
			go func() {
				var result decodedReady
				result.err = json.NewDecoder(reader).Decode(&result.ready)
				ready <- result
			}()
			started := await.Rcv(t, ready)
			require.NoError(t, started.err)
			require.NotNil(t, started.ready.Pending)
			options.RequestID, options.ProcessBootID = "after-process-crash", "20000000-0000-4000-8000-000000000002"
			require.NoError(t, command.Process.Signal(syscall.SIGSTOP))
			contender, err := OpenGraph(t.Context(), options)
			if contender != nil {
				require.NoError(t, contender.runtime.Close(t.Context()))
			}
			require.Error(t, err, "a live native owner must not be displaced")
			require.Nil(t, contender)
			require.Equal(t, uint64(1), state.Version)
			require.Empty(t, submissions)

			require.NoError(t, command.Process.Kill())
			_, killed := errors.AsType[*exec.ExitError](await.Rcv(t, exited))
			waited = true
			require.True(t, killed)
			otherHost := options
			otherHost.StorageHostID = "10000000-0000-4000-8000-000000000002"
			_, err = OpenGraph(t.Context(), otherHost)
			require.ErrorContains(t, err, "original storage host")
			require.Empty(t, submissions)
			legacyPartial := graphDirectory(options.StateRoot, options.NamespaceID, "checkpoints", 1)
			require.NoError(t, os.Mkdir(legacyPartial, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(legacyPartial, "partial"), []byte("preserve incomplete cooperative cut"), 0o600))
			successor, err := OpenGraph(t.Context(), options)
			if failure == "retry uncommitted seal" {
				require.ErrorContains(t, err, "recovery seal did not commit")
				require.Nil(t, successor)
				require.Len(t, submissions, 1)
				stale, staleErr := native.Open(t.Context(), graphConfiguration(options, 1))
				if stale != nil {
					require.NoError(t, stale.Close(t.Context()))
				}
				require.ErrorContains(t, staleErr, "definition changed")
				successor, err = OpenGraph(t.Context(), options)
				require.Len(t, submissions, 2)
				require.True(t, proto.Equal(submissions[0], submissions[1]), "recovery retries must reuse the exact checkpoint and seal")
			}
			require.NoError(t, err)
			preserved, err := os.ReadFile(filepath.Join(legacyPartial, "partial"))
			require.NoError(t, err)
			require.Equal(t, "preserve incomplete cooperative cut", string(preserved))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				require.NoError(t, successor.runtime.Close(ctx))
			})
			require.Equal(t, uint64(2), successor.owner.Grant.Epoch)
			replayed, err := successor.runtime.Apply(t.Context(), native.PollWatchCommand{SubscriptionID: started.ready.SubscriptionID})
			require.NoError(t, err)
			require.Equal(t, started.ready.Pending, replayed.Event)
			_, err = successor.runtime.Apply(t.Context(), graphCrashSource(options.NamespaceID))
			require.NoError(t, err)
			_, err = successor.runtime.Apply(t.Context(), graphCrashQuery(options.NamespaceID, started.ready.SourceID))
			require.NoError(t, err)
			require.NoError(t, successor.runtime.Ack(t.Context(), replayed.Event.Receipt("accepted-after-process-crash")))
			_, err = successor.runtime.Apply(t.Context(), graphCrashFeed(started.ready.SourceID, 2, "after-crash"))
			require.NoError(t, err)
			await.Require(t.Context(), t, func(t *await.T) {
				next, err := successor.runtime.Apply(t.Context(), native.PollWatchCommand{SubscriptionID: started.ready.SubscriptionID})
				require.NoError(t, err)
				require.NotNil(t, next.Event)
				require.Equal(t, started.ready.Pending.Incarnation, next.Event.Incarnation)
				require.Equal(t, started.ready.Pending.Sequence+1, next.Event.Sequence)
				require.Len(t, next.Event.Changes, 1)
			}, 5*time.Second, 10*time.Millisecond)
			require.NoError(t, successor.Close(t.Context()))
		})
	}
}

func graphCrashOptions(root string, client assignmentpb.AssignmentServiceClient) GraphOptions {
	return GraphOptions{
		TemporalInput: ManualTemporalInput{},
		NamespaceID:   "00000000-0000-4000-8000-000000000001", Namespace: "ns", StateRoot: root,
		StorageHostID: "10000000-0000-4000-8000-000000000001",
		ProcessBootID: "20000000-0000-4000-8000-000000000001",
		RequestID:     "graph-begin", Assignments: client,
	}
}

func graphCrashSource(namespaceID string) native.AdmitNodeCommand {
	return native.AdmitNodeCommand{Definition: native.NodeDefinition{
		Lifetime:   native.ResourceLifetime{NamespaceID: namespaceID, ResourceID: "crash.source", Incarnation: "7"},
		Activation: 1, Recipe: native.TemporalSourceRecipe{},
	}}
}

func graphCrashQuery(namespaceID, sourceID string) native.AdmitNodeCommand {
	return native.AdmitNodeCommand{Definition: native.NodeDefinition{
		Lifetime:   native.ResourceLifetime{NamespaceID: namespaceID, ResourceID: "crash.query", Incarnation: "11"},
		Activation: 1,
		Recipe: native.QueryRecipe{
			Language: native.Cypher, Text: "MATCH (n:Item) RETURN n.name AS name", Sources: []string{sourceID},
		},
	}}
}

func graphCrashFeed(sourceID string, position uint64, name string) native.FeedCommand {
	kind := native.Insert
	if position > 1 {
		kind = native.Update
	}
	return native.FeedCommand{
		SourceID: sourceID, Position: position,
		Change: native.Change{
			Kind: kind, ElementID: "item", Labels: []string{"Item"}, EffectiveFrom: position,
			Properties: map[string]any{"name": name},
		},
	}
}
