//go:build temporal_with_drasi && cgo

package watch

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	assignmentpb "go.temporal.io/server/chasm/lib/watchassignment/gen/assignmentpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/watch/native"
)

func TestGraphNativeCloseFailureCannotBecomeASeal(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"cancelled close", "checkpoint destination exists"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			seals := 0
			client := &graphAssignmentClient{
				describe: func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
					return nil, serviceerror.NewNotFound("no assignment")
				},
				begin: func(_ context.Context, request *assignmentpb.BeginRequest) (*assignmentpb.State, error) {
					return graphActiveState(t, request), nil
				},
				seal: func(context.Context, *assignmentpb.SealRequest) (*assignmentpb.State, error) {
					seals++
					return nil, serviceerror.NewInternal("failed native close must not seal")
				},
			}
			options := graphTestOptions(t, client)
			graph, err := OpenGraph(testcontext.For(t), options)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(testcontext.For(t))
			defer cancel()
			if failure == "cancelled close" {
				cancel()
			} else {
				require.NoError(t, os.Mkdir(graph.destination, 0o700))
			}
			first := graph.Close(ctx)
			require.Error(t, first)
			if failure == "cancelled close" {
				require.ErrorIs(t, first, context.Canceled)
			} else {
				require.ErrorContains(t, first, "must be a new absolute directory")
			}
			require.Equal(t, first, graph.Close(testcontext.For(t)))
			require.Zero(t, seals)
			_, err = graph.runtime.Apply(testcontext.For(t), native.ObserveNodesCommand{})
			require.ErrorIs(t, err, native.ErrClosed)
			reopened, err := OpenGraph(testcontext.For(t), options)
			if reopened != nil {
				require.NoError(t, reopened.runtime.Close(testcontext.For(t)))
			}
			require.ErrorContains(t, err, "close has begun")
			require.Nil(t, reopened)
		})
	}
}

func TestGraphNativeSealRetryReusesTheExactClosedCheckpoint(t *testing.T) {
	t.Parallel()
	var state *assignmentpb.State
	var submissions []*assignmentpb.SealRequest
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
			submissions = append(submissions, common.CloneProto(request))
			if len(submissions) == 1 {
				return nil, serviceerror.NewUnavailable("Seal did not commit")
			}
			state = graphSealedState(t, state.GetActive(), request)
			return nil, serviceerror.NewUnavailable("Seal reply lost after commit")
		},
	}
	options := graphTestOptions(t, client)
	graph, err := OpenGraph(testcontext.For(t), options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, graph.runtime.Close(testcontext.For(t))) })
	require.ErrorContains(t, graph.Close(testcontext.For(t)), "Seal did not commit")
	reopened, err := OpenGraph(testcontext.For(t), options)
	if reopened != nil {
		require.NoError(t, reopened.runtime.Close(testcontext.For(t)))
	}
	require.ErrorContains(t, err, "close has begun")
	require.Nil(t, reopened)
	intruder, err := native.Open(testcontext.For(t), native.Config{
		InstanceID: options.NamespaceID, OwnerEpoch: 1, StartMode: native.StartModeDormant,
		StateDir: graphDirectory(graph.options.StateRoot, options.NamespaceID, "owners", 1),
	})
	if intruder != nil {
		require.NoError(t, intruder.Close(testcontext.For(t)))
	}
	require.ErrorContains(t, err, "already owned")
	require.Nil(t, intruder)
	require.NoError(t, graph.Close(testcontext.For(t)))
	require.Len(t, submissions, 2)
	protorequire.ProtoEqual(t, submissions[0], submissions[1])
	_, err = readGraphCheckpoint(state.GetSealed().Checkpoint)
	require.NoError(t, err)
	state = nil
	require.NoError(t, graph.Close(testcontext.For(t)))
	require.Len(t, submissions, 2)
}

func TestGraphNativeValidatesOptionsBeforeAssignment(t *testing.T) {
	t.Parallel()
	graph, err := OpenGraph(testcontext.For(t), GraphOptions{})
	require.Nil(t, graph)
	_, invalid := errors.AsType[*serviceerror.InvalidArgument](err)
	require.True(t, invalid)
}

func TestGraphNativeSuccessorRestoresDormantNodesAndExactPendingOutput(t *testing.T) {
	t.Parallel()
	var state *assignmentpb.State
	client := &graphAssignmentClient{
		describe: func(context.Context, *assignmentpb.DescribeRequest) (*assignmentpb.State, error) {
			if state == nil {
				return nil, serviceerror.NewNotFound("no assignment")
			}
			return common.CloneProto(state), nil
		},
		begin: func(_ context.Context, request *assignmentpb.BeginRequest) (*assignmentpb.State, error) {
			var cut *assignmentpb.Cut
			epoch := uint64(1)
			if sealed := state.GetSealed(); sealed != nil {
				cut, epoch = common.CloneProto(sealed.Checkpoint), sealed.Predecessor.Grant.Epoch+1
			}
			state = graphActiveState(t, request)
			state.GetActive().Grant.Epoch, state.GetActive().RecoveryCut = epoch, cut
			return common.CloneProto(state), nil
		},
		seal: func(_ context.Context, request *assignmentpb.SealRequest) (*assignmentpb.State, error) {
			state = graphSealedState(t, state.GetActive(), request)
			return common.CloneProto(state), nil
		},
	}
	ctx := testcontext.For(t)
	options := graphTestOptions(t, client)
	graph, err := OpenGraph(ctx, options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, graph.runtime.Close(testcontext.For(t))) })
	source := native.AdmitNodeCommand{Definition: native.NodeDefinition{
		Lifetime:   native.ResourceLifetime{NamespaceID: options.NamespaceID, ResourceID: "feed.events", Incarnation: "7"},
		Activation: 1, Recipe: native.TemporalSourceRecipe{},
	}}
	admitted, err := graph.runtime.Apply(ctx, source)
	require.NoError(t, err)
	require.NotNil(t, admitted.Node)
	sourceID := admitted.Node.NativeID
	query := native.AdmitNodeCommand{Definition: native.NodeDefinition{
		Lifetime:   native.ResourceLifetime{NamespaceID: options.NamespaceID, ResourceID: "saved.query", Incarnation: "11"},
		Activation: 1,
		Recipe: native.QueryRecipe{
			Language: native.Cypher, Text: "MATCH (n:Item) RETURN n.name AS name", Sources: []string{sourceID},
		},
	}}
	admitted, err = graph.runtime.Apply(ctx, query)
	require.NoError(t, err)
	require.NotNil(t, admitted.Node)
	queryID := admitted.Node.NativeID
	feed := native.FeedCommand{
		SourceID: sourceID, Position: 1,
		Change: native.Change{
			Kind: native.Insert, ElementID: "item", Labels: []string{"Item"}, EffectiveFrom: 1,
			Properties: map[string]any{"name": "before-handoff"},
		},
	}
	_, err = graph.runtime.Apply(ctx, feed)
	require.NoError(t, err)
	await.Require(ctx, t, func(t *await.T) {
		result, err := graph.runtime.Apply(t.Context(), native.SnapshotCommand{QueryID: queryID})
		require.NoError(t, err)
		require.NotNil(t, result.Snapshot)
		require.Len(t, result.Snapshot.Rows, 1)
	}, 5*time.Second, 10*time.Millisecond)
	bound, err := graph.runtime.Apply(ctx, native.BindWatchCommand{
		QueryID: queryID,
		Binding: native.WatchBinding{
			Watch:         native.ResourceLifetime{NamespaceID: options.NamespaceID, ResourceID: "recurring.watch", Incarnation: "23"},
			WatchRevision: 23, Query: query.Definition.Lifetime, QueryRevision: 11,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, bound.Subscription)
	pending, err := graph.runtime.Apply(ctx, native.PollWatchCommand{SubscriptionID: bound.Subscription.Definition.ID})
	require.NoError(t, err)
	require.NotNil(t, pending.Event)
	require.NoError(t, graph.Close(ctx))
	options.RequestID, options.ProcessBootID = "successor-begin", "20000000-0000-4000-8000-000000000002"
	successor, err := OpenGraph(ctx, options)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, successor.runtime.Close(testcontext.For(t))) })
	nodes, err := successor.runtime.Apply(ctx, native.ObserveNodesCommand{})
	require.NoError(t, err)
	require.Len(t, nodes.Nodes, 2)
	for _, node := range nodes.Nodes {
		require.Equal(t, native.NodeStopped, node.Status)
	}
	retained, err := successor.runtime.Apply(ctx, native.ListSubscriptionsCommand{Limit: 64})
	require.NoError(t, err)
	require.Equal(t, []native.SubscriptionObservation{*bound.Subscription}, retained.Subscriptions)
	replayed, err := successor.runtime.Apply(ctx, native.PollWatchCommand{SubscriptionID: bound.Subscription.Definition.ID})
	require.NoError(t, err)
	require.Equal(t, pending.Event, replayed.Event)
	_, err = successor.runtime.Apply(ctx, source)
	require.NoError(t, err)
	_, err = successor.runtime.Apply(ctx, query)
	require.NoError(t, err)
	require.NoError(t, successor.runtime.Ack(ctx, replayed.Event.Receipt("accepted-before-next-output")))
	feed.Position, feed.Change.Kind, feed.Change.EffectiveFrom = 2, native.Update, 2
	feed.Change.Properties["name"] = "after-handoff"
	_, err = successor.runtime.Apply(ctx, feed)
	require.NoError(t, err)
	await.Require(ctx, t, func(t *await.T) {
		next, err := successor.runtime.Apply(t.Context(), native.PollWatchCommand{SubscriptionID: bound.Subscription.Definition.ID})
		require.NoError(t, err)
		require.NotNil(t, next.Event)
		require.Equal(t, pending.Event.Incarnation, next.Event.Incarnation)
		require.Equal(t, pending.Event.Sequence+1, next.Event.Sequence)
		require.Len(t, next.Event.Changes, 1)
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, successor.Close(ctx))
}
