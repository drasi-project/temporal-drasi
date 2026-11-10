package executionfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const CaptureGapReason = "EXECUTION_FEED_CAPTURE_DISCONTINUITY"

func CaptureDiscontinuity(message string) error {
	result, err := status.New(codes.FailedPrecondition, message).WithDetails(&errdetails.ErrorInfo{
		Reason: CaptureGapReason, Domain: "temporal.execution_feed",
	})
	if err != nil {
		return serviceerror.NewInternal("encode execution feed capture discontinuity: " + err.Error())
	}
	return result.Err()
}

// Cursor is an exclusive per-shard cut, not a global transaction or wall-clock cut.
type Cursor map[int32]int64

// Shard supplies the allocation-aware watermark and the ownership fence.
type Shard interface {
	GetShardID() int32
	GetRangeID() int64
	GetQueueExclusiveHighReadWatermark(tasks.Category) tasks.Key
}

// Store uses the native persistence task tables on SQL and Cassandra.
// Journal category rows must never be passed to normal queue garbage collection.
type Store interface {
	AddHistoryTasks(context.Context, *persistence.AddHistoryTasksRequest) error
	GetHistoryTasks(context.Context, *persistence.GetHistoryTasksRequest) (*persistence.GetHistoryTasksResponse, error)
	CompleteHistoryTask(context.Context, *persistence.CompleteHistoryTaskRequest) error
}

type Processor struct{ Store Store }

// Drain serializes ingestion within one shard. The caller must not run concurrent
// drains for a shard. RangeID fences a replaced owner at journal publication.
// A returned cut is safe only when caughtUp is true; incomplete batches retain
// their cursor. Restarting at zero is safe and does not require a separate checkpoint.
func (p Processor) Drain(ctx context.Context, shard Shard, batchSize int) (cut int64, caughtUp bool, err error) {
	if batchSize <= 0 {
		return 0, false, serviceerror.NewInvalidArgument("execution feed batch size must be positive")
	}
	cut = shard.GetQueueExclusiveHighReadWatermark(tasks.CategoryExecutionFeed).TaskID
	response, err := p.Store.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID: shard.GetShardID(), TaskCategory: tasks.CategoryExecutionFeed,
		InclusiveMinTaskKey: tasks.NewImmediateKey(0), ExclusiveMaxTaskKey: tasks.NewImmediateKey(cut), BatchSize: batchSize,
	})
	if err != nil {
		return 0, false, err
	}
	last := int64(-1)
	for _, task := range response.Tasks {
		feed, ok := task.(*tasks.ExecutionFeedTask)
		if !ok || feed.TaskID <= last || feed.TaskID >= cut {
			return 0, false, serviceerror.NewDataLoss("execution feed source is not ordered within the safe watermark")
		}
		if err := p.append(ctx, shard, feed); err != nil {
			return 0, false, err
		}
		// Deleting the source task is the acknowledgment; journal acceptance precedes it.
		if err := p.Store.CompleteHistoryTask(ctx, &persistence.CompleteHistoryTaskRequest{
			ShardID: shard.GetShardID(), TaskCategory: tasks.CategoryExecutionFeed, TaskKey: feed.GetKey(),
		}); err != nil {
			return 0, false, err
		}
		last = feed.TaskID
	}
	return cut, len(response.NextPageToken) == 0 && len(response.Tasks) < batchSize, nil
}

func (p Processor) append(ctx context.Context, shard Shard, source *tasks.ExecutionFeedTask) error {
	found, err := p.existing(ctx, shard, source)
	if err != nil || found {
		return err
	}
	journal := *source
	journal.Journal = true
	err = p.Store.AddHistoryTasks(ctx, &persistence.AddHistoryTasksRequest{
		ShardID: shard.GetShardID(), RangeID: shard.GetRangeID(), NamespaceID: source.NamespaceID,
		WorkflowID: source.WorkflowID, Tasks: map[tasks.Category][]tasks.Task{tasks.CategoryExecutionFeedJournal: {&journal}},
		ArchetypeID: source.ArchetypeID,
	})
	if err == nil {
		return nil
	}
	// SQL reports an existing key as an insert error. A lost commit response
	// follows the same path. Compare the immutable record before accepting it.
	found, readErr := p.existing(ctx, shard, source)
	if readErr != nil {
		return readErr
	}
	if !found {
		return err
	}
	return nil
}

func (p Processor) existing(ctx context.Context, shard Shard, source *tasks.ExecutionFeedTask) (bool, error) {
	existing, readErr := p.Store.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID: shard.GetShardID(), TaskCategory: tasks.CategoryExecutionFeedJournal, BatchSize: 1,
		InclusiveMinTaskKey: source.GetKey(), ExclusiveMaxTaskKey: tasks.NewImmediateKey(source.TaskID + 1),
	})
	if readErr != nil {
		return false, readErr
	}
	if len(existing.Tasks) == 0 {
		return false, nil
	}
	actual, ok := existing.Tasks[0].(*tasks.ExecutionFeedTask)
	if !ok {
		return false, serviceerror.NewDataLoss("invalid execution feed journal record")
	}
	expectedBytes, encodeErr := json.Marshal(source)
	if encodeErr != nil {
		return false, encodeErr
	}
	actualBytes, encodeErr := json.Marshal(actual)
	if encodeErr != nil {
		return false, encodeErr
	}
	if !slices.Equal(expectedBytes, actualBytes) {
		return false, serviceerror.NewDataLoss("execution feed task identity has conflicting contents")
	}
	return true, nil
}

type Reader struct {
	Store        Store
	NamespaceID  string
	CaptureEpoch int64
}

// Read reads a bounded immutable interval [from, through) on one shard.
// Through must be a cut returned by a caught-up Processor, never the highest
// observed task ID. A caller advances from only after durably accepting results.
func (r Reader) Read(ctx context.Context, shardID int32, from, through int64, limit int) ([]FeedRecord, int64, error) {
	if r.NamespaceID == "" || shardID < 0 || from < 0 || through < from || limit <= 0 {
		return nil, from, serviceerror.NewInvalidArgument("invalid execution feed replay range")
	}
	if from == through {
		return nil, through, nil
	}
	page, err := r.Store.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID: shardID, TaskCategory: tasks.CategoryExecutionFeedJournal, BatchSize: limit,
		InclusiveMinTaskKey: tasks.NewImmediateKey(from), ExclusiveMaxTaskKey: tasks.NewImmediateKey(through),
	})
	if err != nil {
		return nil, from, err
	}
	result := make([]FeedRecord, 0, len(page.Tasks))
	next := from
	for _, task := range page.Tasks {
		record, ok := task.(*tasks.ExecutionFeedTask)
		if !ok || !record.ValidSchema() || record.TaskID < next || record.TaskID >= through {
			return nil, from, serviceerror.NewDataLoss("invalid execution feed replay record")
		}
		if record.NamespaceID == r.NamespaceID {
			if r.CaptureEpoch > 0 && record.CaptureEpoch != r.CaptureEpoch {
				return nil, from, CaptureDiscontinuity("execution feed record belongs to a different capture session")
			}
			if record.CaptureGap {
				return nil, from, CaptureDiscontinuity("execution feed capture was interrupted; a new source reconstruction is required")
			}
			result = append(result, FeedRecord{ShardID: shardID, ExecutionFeedTask: record})
		}
		next = record.TaskID + 1
	}
	if len(page.NextPageToken) == 0 && len(page.Tasks) < limit {
		next = through
	}
	return result, next, nil
}

// Snapshot is reconstructed exclusively from immutable journal versions through
// its paired vector. Repeating the read yields the same state even as writers run.
// No journal retention/GC is enabled; introducing it requires explicit replay leases.
type Snapshot struct {
	Cursor     Cursor                                     `json:"cursor"`
	Executions map[definition.WorkflowKey]tasks.FeedState `json:"-"`
}

func (r Reader) Snapshot(ctx context.Context, cut Cursor, maxExecutions int) (Snapshot, error) {
	if r.NamespaceID == "" || len(cut) == 0 || maxExecutions <= 0 {
		return Snapshot{}, serviceerror.NewInvalidArgument("execution feed snapshot requires a namespace, a nonempty vector, and a positive limit")
	}
	for shard, offset := range cut {
		if shard < 0 || offset < 0 {
			return Snapshot{}, serviceerror.NewInvalidArgument("invalid execution feed snapshot cursor")
		}
	}
	result := Snapshot{Cursor: maps.Clone(cut), Executions: make(map[definition.WorkflowKey]tasks.FeedState)}
	for _, shard := range slices.Sorted(maps.Keys(cut)) {
		for from := int64(0); from < cut[shard]; {
			page, next, err := r.Read(ctx, shard, from, cut[shard], 1000)
			if err != nil {
				return Snapshot{}, err
			}
			if next <= from {
				return Snapshot{}, fmt.Errorf("execution feed snapshot made no progress on shard %d", shard)
			}
			for _, record := range page {
				if record.Deleted {
					delete(result.Executions, record.WorkflowKey)
				} else {
					result.Executions[record.WorkflowKey] = record.State
				}
			}
			if len(result.Executions) > maxExecutions {
				return Snapshot{}, serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "execution feed snapshot execution limit exceeded")
			}
			from = next
		}
	}
	return result, nil
}
