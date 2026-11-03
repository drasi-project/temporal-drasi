package executionfeed

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
)

type testShard struct{ high int64 }

func (s testShard) GetShardID() int32 { return 1 }
func (s testShard) GetRangeID() int64 { return 2 }
func (s testShard) GetQueueExclusiveHighReadWatermark(tasks.Category) tasks.Key {
	return tasks.NewImmediateKey(s.high)
}

type testStore struct {
	source             map[int64]tasks.Task
	journal            map[int64]tasks.Task
	appendError        error
	loseAppendResponse bool
	loseAckResponse    bool
	executionExists    bool
	acks               int
}

func (s *testStore) AddHistoryTasks(_ context.Context, r *persistence.AddHistoryTasksRequest) error {
	if s.appendError != nil {
		return s.appendError
	}
	for _, task := range r.Tasks[tasks.CategoryExecutionFeedJournal] {
		if _, exists := s.journal[task.GetTaskID()]; exists {
			return errors.New("duplicate key")
		}
		// Exercise the real on-disk serializer, including schema/encoding checks.
		serializer := serialization.NewSerializer()
		blob, err := serializer.SerializeTask(task)
		if err != nil {
			return err
		}
		stored, err := serializer.DeserializeTask(tasks.CategoryExecutionFeedJournal, blob)
		if err != nil {
			return err
		}
		s.journal[task.GetTaskID()] = stored
	}
	if s.loseAppendResponse {
		s.loseAppendResponse = false
		return context.DeadlineExceeded
	}
	return nil
}

func (s *testStore) GetHistoryTasks(_ context.Context, r *persistence.GetHistoryTasksRequest) (*persistence.GetHistoryTasksResponse, error) {
	data := s.source
	if r.TaskCategory == tasks.CategoryExecutionFeedJournal {
		data = s.journal
	}
	var ids []int64
	for id := range data {
		if id >= r.InclusiveMinTaskKey.TaskID && id < r.ExclusiveMaxTaskKey.TaskID {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	response := &persistence.GetHistoryTasksResponse{}
	for _, id := range ids {
		if len(response.Tasks) == r.BatchSize {
			response.NextPageToken = []byte{1}
			break
		}
		response.Tasks = append(response.Tasks, data[id])
	}
	return response, nil
}

func (s *testStore) CompleteHistoryTask(_ context.Context, r *persistence.CompleteHistoryTaskRequest) error {
	if _, exists := s.journal[r.TaskKey.TaskID]; !exists {
		return errors.New("ack preceded durable append")
	}
	delete(s.source, r.TaskKey.TaskID)
	s.acks++
	if s.loseAckResponse {
		s.loseAckResponse = false
		return context.DeadlineExceeded
	}
	return nil
}

func (s *testStore) GetWorkflowExecution(context.Context, *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
	if s.executionExists {
		return &persistence.GetWorkflowExecutionResponse{}, nil
	}
	return nil, serviceerror.NewNotFound("deleted")
}

func newTestStore() *testStore {
	return &testStore{source: make(map[int64]tasks.Task), journal: make(map[int64]tasks.Task)}
}

func record(id int64, status string) *tasks.ExecutionFeedTask {
	return &tasks.ExecutionFeedTask{TaskID: id, SchemaVersion: 1,
		WorkflowKey: definition.NewWorkflowKey("namespace", "workflow", "run"),
		State:       tasks.FeedState{Activities: []tasks.FeedActivity{{ID: "activity", Attempt: 1, Status: status}}},
	}
}

func TestExecutionFeedCommitLossRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		appendLoss, ackLoss bool
	}{
		{name: "journal response lost", appendLoss: true},
		{name: "source acknowledgment response lost", ackLoss: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore()
			store.source[10] = record(10, "STARTED")
			store.loseAppendResponse, store.loseAckResponse = tc.appendLoss, tc.ackLoss
			processor := Processor{Store: store}
			_, _, err := processor.Drain(t.Context(), testShard{high: 20}, 100)
			if tc.ackLoss {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.NoError(t, err)
			}
			cut, caughtUp, err := processor.Drain(t.Context(), testShard{high: 20}, 100)
			require.NoError(t, err)
			require.True(t, caughtUp)
			require.Equal(t, int64(20), cut)
			require.Len(t, store.journal, 1)
			require.Empty(t, store.source)
			require.Equal(t, 1, store.acks)
		})
	}
}

func TestExecutionFeedNoAckBeforeCommit(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	store.source[10] = record(10, "STARTED")
	store.appendError = context.DeadlineExceeded
	_, _, err := (Processor{Store: store}).Drain(t.Context(), testShard{high: 20}, 100)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, store.source, 1)
	require.Empty(t, store.journal)
	require.Zero(t, store.acks)
}

func TestExecutionFeedCaptureGapRefusesCursorAdvancement(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	first := record(10, "STARTED")
	first.SchemaVersion, first.CaptureEpoch = 2, 7
	store.source[10] = first
	store.source[20] = &tasks.ExecutionFeedTask{
		WorkflowKey: first.WorkflowKey, TaskID: 20, SchemaVersion: 2, CaptureEpoch: 7, CaptureGap: true,
	}
	_, caughtUp, err := (Processor{Store: store}).Drain(t.Context(), testShard{high: 30}, 100)
	require.NoError(t, err)
	require.True(t, caughtUp)
	reader := Reader{Store: store, NamespaceID: first.NamespaceID, CaptureEpoch: 7}
	page, next, err := reader.Read(t.Context(), 1, 0, 30, 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, int64(11), next)
	page, next, err = reader.Read(t.Context(), 1, next, 30, 100)
	require.ErrorContains(t, err, "capture was interrupted")
	require.Nil(t, page)
	require.Equal(t, int64(11), next)
}

func TestExecutionFeedCaptureEpochCannotReadLegacyOrOtherSession(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		epoch int64
	}{
		{name: "legacy unscoped record"},
		{name: "different capture session", epoch: 6},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore()
			task := record(10, "STARTED")
			if test.epoch != 0 {
				task.SchemaVersion, task.CaptureEpoch = 2, test.epoch
			}
			store.source[10] = task
			_, _, err := (Processor{Store: store}).Drain(t.Context(), testShard{high: 20}, 100)
			require.NoError(t, err)
			page, next, err := (Reader{Store: store, NamespaceID: task.NamespaceID, CaptureEpoch: 7}).Read(t.Context(), 1, 0, 20, 100)
			require.ErrorContains(t, err, "different capture session")
			require.Nil(t, page)
			require.Zero(t, next)
		})
	}
}

func TestExecutionFeedWatermarkAndImmutableSnapshot(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	store.source[10], store.source[30] = record(10, "SCHEDULED"), record(30, "STARTED")
	processor := Processor{Store: store}
	cut, caughtUp, err := processor.Drain(t.Context(), testShard{high: 30}, 100)
	require.NoError(t, err)
	require.True(t, caughtUp)
	require.Len(t, store.source, 1)
	require.Equal(t, int64(30), cut)
	reader := Reader{Store: store, NamespaceID: "namespace"}
	first, err := reader.Snapshot(t.Context(), Cursor{1: cut}, 10)
	require.NoError(t, err)
	_, _, err = processor.Drain(t.Context(), testShard{high: 40}, 100)
	require.NoError(t, err)
	again, err := reader.Snapshot(t.Context(), Cursor{1: cut}, 10)
	require.NoError(t, err)
	require.Equal(t, first, again)
	tail, next, err := reader.Read(t.Context(), 1, cut, 40, 100)
	require.NoError(t, err)
	require.Equal(t, int64(40), next)
	require.Len(t, tail, 1)
	require.Equal(t, int64(30), tail[0].TaskID)
}

func TestExecutionFeedTombstone(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	deleted := record(10, "")
	deleted.Deleted = true
	store.source[10] = deleted
	processor := Processor{Store: store}
	_, caughtUp, err := processor.Drain(t.Context(), testShard{high: 20}, 100)
	require.NoError(t, err)
	require.True(t, caughtUp)
	snapshot, err := (Reader{Store: store, NamespaceID: "namespace"}).Snapshot(t.Context(), Cursor{1: 20}, 10)
	require.NoError(t, err)
	require.Empty(t, snapshot.Executions)
}

func TestExecutionFeedConflictingJournalIdentity(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	store.source[10] = record(10, "STARTED")
	store.journal[10] = record(10, "SCHEDULED")
	_, _, err := (Processor{Store: store}).Drain(t.Context(), testShard{high: 20}, 100)
	require.ErrorContains(t, err, "conflicting contents")
	require.Zero(t, store.acks)
	require.Equal(t, "SCHEDULED", store.journal[10].(*tasks.ExecutionFeedTask).State.Activities[0].Status)
}

func TestExecutionFeedNamespaceReader(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	store.source[10] = record(10, "SCHEDULED")
	other := record(20, "STARTED")
	other.NamespaceID = "other"
	store.source[20] = other
	_, _, err := (Processor{Store: store}).Drain(t.Context(), testShard{high: 30}, 100)
	require.NoError(t, err)
	reader := Reader{Store: store, NamespaceID: "namespace"}
	page, next, err := reader.Read(t.Context(), 1, 0, 30, 100)
	require.NoError(t, err)
	require.Equal(t, int64(30), next)
	require.Len(t, page, 1)
	require.Equal(t, "namespace", page[0].NamespaceID)
	snapshot, err := reader.Snapshot(t.Context(), Cursor{1: 30}, 10)
	require.NoError(t, err)
	require.Len(t, snapshot.Executions, 1)
	_, err = reader.Snapshot(t.Context(), nil, 10)
	require.Error(t, err)
}

func TestExecutionFeedBootstrapRace(t *testing.T) {
	t.Parallel()
	// Each baseline is captured under the execution lock. A retry of a page
	// captures the newer state at a newer task ID, never the old scan's state.
	store := newTestStore()
	store.source[10] = record(10, "SCHEDULED")
	store.source[20] = record(20, "STARTED")
	baseline := record(30, "STARTED")
	baseline.Baseline = true
	store.source[30] = baseline
	_, _, err := (Processor{Store: store}).Drain(t.Context(), testShard{high: 40}, 100)
	require.NoError(t, err)
	snapshot, err := (Reader{Store: store, NamespaceID: "namespace"}).Snapshot(t.Context(), Cursor{1: 40}, 10)
	require.NoError(t, err)
	require.Equal(t, baseline.State, snapshot.Executions[baseline.WorkflowKey])
}
