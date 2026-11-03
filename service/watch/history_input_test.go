package watch

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/history/executionfeed"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/grpc"
)

type historyRuntimeFunc func(context.Context, native.Command) (native.Result, error)

func (apply historyRuntimeFunc) Apply(ctx context.Context, command native.Command) (native.Result, error) {
	return apply(ctx, command)
}

type historyFeedTestClient struct {
	historyFeedClient
	bootstrap func(context.Context, *historyservice.BootstrapExecutionFeedRequest) (*historyservice.BootstrapExecutionFeedResponse, error)
	read      func(context.Context, *historyservice.GetExecutionFeedRequest) (*historyservice.GetExecutionFeedResponse, error)
}

func (client historyFeedTestClient) BootstrapExecutionFeed(ctx context.Context, request *historyservice.BootstrapExecutionFeedRequest, _ ...grpc.CallOption) (*historyservice.BootstrapExecutionFeedResponse, error) {
	return client.bootstrap(ctx, request)
}

func (client historyFeedTestClient) GetExecutionFeed(ctx context.Context, request *historyservice.GetExecutionFeedRequest, _ ...grpc.CallOption) (*historyservice.GetExecutionFeedResponse, error) {
	return client.read(ctx, request)
}

func historyTestRecord() executionfeed.FeedRecord {
	return executionfeed.FeedRecord{ShardID: 1, ExecutionFeedTask: &tasks.ExecutionFeedTask{
		WorkflowKey: definition.NewWorkflowKey("00000000-0000-4000-8000-000000000001", "workflow", "run"),
		TaskID:      10, SchemaVersion: 2, CaptureEpoch: 7, VisibilityTimestamp: time.UnixMilli(1_700_000_000_000).UTC(),
		State: tasks.FeedState{Workflow: &tasks.FeedWorkflow{
			Type: "type", TaskQueue: "queue", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}},
	}}
}

func TestHistoryProjectionPreservesTypedValuesAndRelationships(t *testing.T) {
	t.Parallel()
	record := historyTestRecord()
	record.State.Workflow.SearchAttributes = map[string]tasks.FeedSearchAttribute{
		"LargeInteger": {Type: enumspb.INDEXED_VALUE_TYPE_INT, Value: json.RawMessage("9007199254740993")},
	}
	record.State.Activities = []tasks.FeedActivity{{ID: "activity", ScheduledEventID: 3, Attempt: 2, Status: "STARTED"}}
	record.State.Timers = []tasks.FeedTimer{{ID: "timer", StartedEventID: 4, Status: "SCHEDULED"}}
	record.Facts = []tasks.FeedFact{{Entity: "ACTIVITY", ID: "activity", ScheduledEventID: 3, Attempt: 2, Transition: "STARTED"}}
	payload, err := json.Marshal(record)
	require.NoError(t, err)
	image, err := historyRecord("source", native.HistoryScope{NamespaceID: record.NamespaceID, ShardCount: 1}, 1, 7, payload)
	require.NoError(t, err)
	require.Len(t, image.State, 5)
	prefix := "x:" + image.ExecutionKey + ":state:"
	require.Equal(t, map[string]any{
		"namespace_id": record.NamespaceID, "workflow_id": "workflow", "run_id": "run",
		"data": map[string]any{
			"type": "type", "task_queue": "queue",
			"state": record.State.Workflow.State.String(), "status": record.State.Workflow.Status.String(),
			"search_attributes": map[string]any{
				"LargeInteger": map[string]any{"type": enumspb.INDEXED_VALUE_TYPE_INT.String(), "value": json.Number("9007199254740993")},
			},
		},
	}, image.State[0].Properties)
	require.Equal(t, &native.Relation{
		InSource: "source", InID: prefix + "workflow", OutSource: "source", OutID: prefix + "activity:3",
	}, image.State[2].Relation)
	require.Equal(t, &native.Relation{
		InSource: "source", InID: prefix + "workflow", OutSource: "source", OutID: prefix + "timer:4",
	}, image.State[4].Relation)
	require.Len(t, image.Facts, 1)
	require.Equal(t, "x:"+image.ExecutionKey+":fact:10:0", image.Facts[0].ElementID)
	record.Deleted, record.State = true, tasks.FeedState{}
	payload, err = json.Marshal(record)
	require.NoError(t, err)
	deleted, err := historyRecord("source", native.HistoryScope{NamespaceID: record.NamespaceID, ShardCount: 1}, 1, 7, payload)
	require.NoError(t, err)
	require.Empty(t, deleted.State)
	require.Equal(t, image.ExecutionKey, deleted.ExecutionKey)
	require.Equal(t, image.Facts, deleted.Facts)
}

func TestHistoryProjectionRejectsInvalidBoundaryData(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*executionfeed.FeedRecord)
	}{
		{"wrong shard", func(record *executionfeed.FeedRecord) { record.ShardID = 2 }},
		{"wrong namespace", func(record *executionfeed.FeedRecord) { record.NamespaceID = "another" }},
		{"wrong epoch", func(record *executionfeed.FeedRecord) { record.CaptureEpoch++ }},
		{"legacy schema", func(record *executionfeed.FeedRecord) { record.SchemaVersion = 1 }},
		{"capture gap", func(record *executionfeed.FeedRecord) { record.CaptureGap = true }},
		{"zero time", func(record *executionfeed.FeedRecord) { record.VisibilityTimestamp = time.Time{} }},
		{"deleted state", func(record *executionfeed.FeedRecord) { record.Deleted = true }},
		{"missing parent", func(record *executionfeed.FeedRecord) {
			record.State = tasks.FeedState{Activities: []tasks.FeedActivity{{ID: "activity", ScheduledEventID: 3}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			record := historyTestRecord()
			test.mutate(&record)
			payload, err := json.Marshal(record)
			require.NoError(t, err)
			_, err = historyRecord("source", native.HistoryScope{NamespaceID: historyTestRecord().NamespaceID, ShardCount: 1}, 1, 7, payload)
			require.Error(t, err)
		})
	}
}

func TestHistoryAcceptanceResolvesLostRepliesWithoutRefetching(t *testing.T) {
	t.Parallel()
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "before commit", true: "after commit"}[committed], func(t *testing.T) {
			t.Parallel()
			step := native.HistoryStep{RequestID: "00000000-0000-4000-8000-000000000002", ShardID: 1, ExpectedRevision: 4}
			receipt := &native.HistoryReceipt{RequestID: step.RequestID, Revision: 5, Head: 10}
			calls := 0
			worker := historyInputWorker{source: "source", runtime: historyRuntimeFunc(func(_ context.Context, command native.Command) (native.Result, error) {
				calls++
				switch command.(type) {
				case native.AcceptHistoryCommand:
					return native.Result{}, serviceerror.NewUnavailable("acceptance response lost")
				case native.HistoryProgressCommand:
					progress := &native.HistoryProgress{Revision: 4}
					if committed {
						progress.Revision, progress.Receipt = 5, receipt
					}
					return native.Result{HistoryProgress: progress}, nil
				default:
					return native.Result{}, serviceerror.NewInvalidArgument("unexpected operation")
				}
			})}
			actual, err := worker.accept(testcontext.For(t), step)
			if committed {
				require.NoError(t, err)
				require.Equal(t, receipt, actual)
			} else {
				require.ErrorContains(t, err, "acceptance response lost")
				require.Nil(t, actual)
			}
			require.Equal(t, 2, calls)
		})
	}
}

func TestHistoryTransportBoundsTheActualCommandWithoutSkippingRecords(t *testing.T) {
	t.Parallel()
	record := historyTestRecord()
	record.State.Workflow.Type = strings.Repeat("x", 600_000)
	first, err := json.Marshal(record)
	require.NoError(t, err)
	record.TaskID++
	second, err := json.Marshal(record)
	require.NoError(t, err)
	var accepted *native.AcceptHistoryCommand
	worker := historyInputWorker{
		source: "source", scope: native.HistoryScope{NamespaceID: record.NamespaceID, ShardCount: 1},
		client: historyFeedTestClient{read: func(_ context.Context, request *historyservice.GetExecutionFeedRequest) (*historyservice.GetExecutionFeedResponse, error) {
			if request.EndPosition == request.StartPosition {
				return &historyservice.GetExecutionFeedResponse{NextPosition: 10, PublishedCutoff: 12}, nil
			}
			return &historyservice.GetExecutionFeedResponse{Records: [][]byte{first, second}, NextPosition: 12, PublishedCutoff: 12}, nil
		}},
		runtime: historyRuntimeFunc(func(_ context.Context, command native.Command) (native.Result, error) {
			switch command := command.(type) {
			case native.HistoryProgressCommand:
				return native.Result{HistoryProgress: &native.HistoryProgress{
					Revision: 1, Cursor: native.HistoryCursor{
						Phase: native.HistoryCapturing, CaptureEpoch: 7, ReconstructionStart: 10, NextPosition: 10,
						Scan: &native.HistoryScan{Phase: native.HistoryScanComplete, Cutoff: 12},
					},
				}}, nil
			case native.AcceptHistoryCommand:
				accepted = &command
				return native.Result{HistoryReceipt: &native.HistoryReceipt{RequestID: command.Step.RequestID, Revision: 2}}, nil
			default:
				return native.Result{}, serviceerror.NewInvalidArgument("unexpected operation")
			}
		}),
	}
	ready, err := worker.advance(testcontext.For(t), 1)
	require.NoError(t, err)
	require.False(t, ready)
	require.NotNil(t, accepted)
	page := accepted.Step.Page.(native.HistoryRecordsPage)
	require.Len(t, page.Records, 1)
	require.Equal(t, int64(11), page.NextPosition)
	require.Equal(t, int64(12), page.PublishedCutoff)
	size, err := native.CommandSize(*accepted)
	require.NoError(t, err)
	require.LessOrEqual(t, size, native.MaxCommandBytes)
}

func TestHistoryFailureDistinguishesGapsFromTemporaryCaptureDisablement(t *testing.T) {
	t.Parallel()
	require.True(t, permanentHistoryFailure(executionfeed.CaptureDiscontinuity("capture interrupted")))
	require.True(t, permanentHistoryFailure(serviceerror.NewDataLoss("invalid source record")))
	require.False(t, permanentHistoryFailure(serviceerror.NewFailedPrecondition("capture is disabled")))
	require.False(t, permanentHistoryFailure(serviceerror.NewUnavailable("owner is moving")))
}

func TestHistoryStopJoinsLateRPCBeforeAllowingSourceStop(t *testing.T) {
	t.Parallel()
	ctx := testcontext.For(t)
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	accepted := 0
	worker := &historyInputWorker{
		source: "source", scope: native.HistoryScope{NamespaceID: historyTestRecord().NamespaceID, ShardCount: 1},
		cancel: cancel, done: make(chan struct{}),
		runtime: historyRuntimeFunc(func(ctx context.Context, command native.Command) (native.Result, error) {
			if err := ctx.Err(); err != nil {
				return native.Result{}, err
			}
			switch command.(type) {
			case native.HistoryProgressCommand:
				return native.Result{HistoryProgress: &native.HistoryProgress{Cursor: native.HistoryCursor{Phase: native.HistoryUnstarted}}}, nil
			case native.AcceptHistoryCommand:
				accepted++
			default:
				return native.Result{}, serviceerror.NewInvalidArgument("unexpected operation")
			}
			return native.Result{}, serviceerror.NewInvalidArgument("unexpected operation")
		}),
		client: historyFeedTestClient{bootstrap: func(context.Context, *historyservice.BootstrapExecutionFeedRequest) (*historyservice.BootstrapExecutionFeedResponse, error) {
			entered <- struct{}{}
			select {
			case <-release:
				return &historyservice.BootstrapExecutionFeedResponse{CaptureEpoch: 7, ReconstructionStart: 10, BootstrapCutoff: 11}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}},
	}
	result := make(chan error, 1)
	go func() {
		result <- worker.run(run)
		close(worker.done)
	}()
	await.Rcv(t, entered)
	graph := &Graph{history: map[string]*historyInputWorker{"source": worker}}
	stopped := make(chan error, 1)
	go func() { stopped <- graph.stopHistoryWorker(ctx, "source") }()
	select {
	case <-run.Done():
	case <-ctx.Done():
		t.Fatal("source stop did not cancel its worker")
	}
	select {
	case err := <-stopped:
		t.Fatalf("source stop preceded the late RPC response: %v", err)
	default:
	}
	close(release)
	require.NoError(t, await.Rcv(t, stopped))
	require.ErrorIs(t, await.Rcv(t, result), context.Canceled)
	require.Zero(t, accepted)
}

func TestHistoryFailureQuiescesOnlyDependentsWithoutMutatingPublishedNodes(t *testing.T) {
	t.Parallel()
	nodes := map[string]native.NodeObservation{
		"source": {NativeID: "source", Status: native.NodeRunning},
		"query": {NativeID: "query", Definition: native.NodeDefinition{
			Recipe: native.QueryRecipe{Sources: []string{"source"}},
		}},
		"healthy-query": {NativeID: "healthy-query", Definition: native.NodeDefinition{
			Recipe: native.QueryRecipe{Sources: []string{"healthy-source"}},
		}},
	}
	failed, healthy := native.ResourceLifetime{ResourceID: "failed"}, native.ResourceLifetime{ResourceID: "healthy"}
	cancelled := false
	graph := Graph{
		nodes: nodes, history: map[string]*historyInputWorker{"source": {problem: "owning-History unavailable"}},
		watches: map[native.ResourceLifetime]*graphWatch{
			failed: {accepting: true, cancel: func() { cancelled = true }, binding: &native.SubscriptionObservation{
				Definition: native.Subscription{QueryID: "query"},
			}},
			healthy: {accepting: true, binding: &native.SubscriptionObservation{
				Definition: native.Subscription{QueryID: "healthy-query"},
			}},
		},
	}
	graph.applyHistoryFailuresLocked()
	require.Equal(t, native.NodeRunning, nodes["source"].Status)
	require.Equal(t, native.NodeFailed, graph.nodes["source"].Status)
	require.True(t, cancelled)
	require.False(t, graph.watches[failed].accepting)
	require.True(t, graph.watches[healthy].accepting)
	graph.watches[failed].quiesced = true
	graph.applyHistoryFailuresLocked()
	require.True(t, graph.watches[failed].quiesced, "an established History pause must not be invalidated by repeated observations")
}
