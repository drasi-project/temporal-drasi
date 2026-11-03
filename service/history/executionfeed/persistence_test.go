//go:build integration

package executionfeed

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func sqliteStore(t *testing.T) persistence.ExecutionManager {
	t.Helper()
	tv := testvars.New(t)
	serializer := serialization.NewSerializer()
	factory := sql.NewFactory(config.SQL{PluginName: "sqlite", DatabaseName: tv.Any().String(),
		ConnectAttributes: map[string]string{"mode": "memory", "cache": "shared"}},
		resolver.NewNoopResolver(), "cluster", log.NewNoopLogger(), metrics.NoopMetricsHandler, serializer)
	t.Cleanup(factory.Close)
	shardStore, err := factory.NewShardStore()
	require.NoError(t, err)
	_, err = persistence.NewShardManager(shardStore, serializer).GetOrCreateShard(t.Context(), &persistence.GetOrCreateShardRequest{
		ShardID: 1, InitialShardInfo: &persistencespb.ShardInfo{ShardId: 1, RangeId: 2}, LifecycleContext: t.Context(),
	})
	require.NoError(t, err)
	executionStore, err := factory.NewExecutionStore()
	require.NoError(t, err)
	return persistence.NewExecutionManager(executionStore, serializer, nil, log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(16*1024*1024), dynamicconfig.GetBoolPropertyFn(false))
}

func TestExecutionFeedPrimaryCommitAndReplay(t *testing.T) {
	t.Parallel()
	manager := sqliteStore(t)
	tv := testvars.New(t)
	tv = tv.WithRunID("10000000-0000-0000-0000-000000000001")
	first := record(10, "SCHEDULED")
	first.WorkflowKey = definition.NewWorkflowKey(tv.NamespaceID().String(), tv.WorkflowID(), tv.RunID())
	first.ArchetypeID = chasm.WorkflowArchetypeID
	info := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: tv.NamespaceID().String(), WorkflowId: tv.WorkflowID(),
		ExecutionStats: &persistencespb.ExecutionStats{},
	}
	state := &persistencespb.WorkflowExecutionState{
		RunId: tv.RunID(), CreateRequestId: tv.RequestID(),
		State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		StartTime: timestamppb.New(time.Unix(100, 0)),
	}
	branchID := tv.RunID()
	branch, err := manager.GetHistoryBranchUtil().NewHistoryBranch(tv.NamespaceID().String(), tv.WorkflowID(), tv.RunID(),
		tv.RunID(), &branchID, nil, 0, 0, 0)
	require.NoError(t, err)
	info.VersionHistories = &historyspb.VersionHistories{Histories: []*historyspb.VersionHistory{{
		BranchToken: branch, Items: []*historyspb.VersionHistoryItem{{EventId: 1, Version: 1}},
	}}}
	_, err = manager.CreateWorkflowExecution(t.Context(), &persistence.CreateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 2, Mode: persistence.CreateWorkflowModeBrandNew, ArchetypeID: chasm.WorkflowArchetypeID,
		NewWorkflowSnapshot: persistence.WorkflowSnapshot{ExecutionInfo: info, ExecutionState: state,
			NextEventID: 2, DBRecordVersion: 1, Tasks: map[tasks.Category][]tasks.Task{tasks.CategoryExecutionFeed: {first}}},
		NewWorkflowEvents: []*persistence.WorkflowEvents{{NamespaceID: first.NamespaceID, WorkflowID: first.WorkflowID,
			RunID: first.RunID, BranchToken: branch, TxnID: 1, Events: []*historypb.HistoryEvent{
				{EventId: 1, Version: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, EventTime: timestamppb.New(time.Unix(100, 0))},
			}}},
	})
	require.NoError(t, err)
	page, err := manager.ListConcreteExecutions(t.Context(), &persistence.ListConcreteExecutionsRequest{ShardID: 1, PageSize: 1, KeysOnly: true})
	require.NoError(t, err)
	require.Equal(t, []definition.WorkflowKey{first.WorkflowKey}, page.Keys)
	require.NotEmpty(t, page.PageToken)
	last, err := manager.ListConcreteExecutions(t.Context(), &persistence.ListConcreteExecutionsRequest{ShardID: 1, PageSize: 1, PageToken: page.PageToken, KeysOnly: true})
	require.NoError(t, err)
	require.Empty(t, last.Keys)
	require.Empty(t, last.PageToken)

	second := record(20, "STARTED")
	second.WorkflowKey = first.WorkflowKey
	second.ArchetypeID = chasm.WorkflowArchetypeID
	info.VersionHistories.Histories[0].Items[0].EventId = 2
	update := &persistence.UpdateWorkflowExecutionRequest{
		ShardID: 1, RangeID: 2, Mode: persistence.UpdateWorkflowModeUpdateCurrent, ArchetypeID: chasm.WorkflowArchetypeID,
		UpdateWorkflowMutation: persistence.WorkflowMutation{ExecutionInfo: info, ExecutionState: state,
			NextEventID: 3, Condition: 2, DBRecordVersion: 999,
			Tasks: map[tasks.Category][]tasks.Task{tasks.CategoryExecutionFeed: {second}}},
		UpdateWorkflowEvents: []*persistence.WorkflowEvents{{NamespaceID: first.NamespaceID, WorkflowID: first.WorkflowID,
			RunID: first.RunID, BranchToken: branch, PrevTxnID: 1, TxnID: 2, Events: []*historypb.HistoryEvent{
				{EventId: 2, Version: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, EventTime: timestamppb.New(time.Unix(101, 0))},
			}}},
	}
	_, err = manager.UpdateWorkflowExecution(t.Context(), update)
	require.Error(t, err)
	// The history append survived a rejected mutable-state commit. It must not be a source event.
	history, err := manager.ReadHistoryBranch(t.Context(), &persistence.ReadHistoryBranchRequest{
		ShardID: 1, BranchToken: branch, MinEventID: 1, MaxEventID: 3, PageSize: 100,
	})
	require.NoError(t, err)
	require.Len(t, history.HistoryEvents, 2)
	processor := Processor{Store: manager}
	_, _, err = processor.Drain(t.Context(), testShard{high: 30}, 100)
	require.NoError(t, err)
	reader := Reader{Store: manager, NamespaceID: first.NamespaceID}
	records, _, err := reader.Read(t.Context(), 1, 0, 30, 100)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, int64(10), records[0].TaskID)
	update.UpdateWorkflowMutation.DBRecordVersion = 2
	_, err = manager.UpdateWorkflowExecution(t.Context(), update)
	require.NoError(t, err)
	_, _, err = processor.Drain(t.Context(), testShard{high: 30}, 100)
	require.NoError(t, err)
	// A new reader reconstructs from persistent records, not process-local projection state.
	snapshot, err := (Reader{Store: manager, NamespaceID: first.NamespaceID}).Snapshot(t.Context(), Cursor{1: 30}, 10)
	require.NoError(t, err)
	require.Equal(t, second.State, snapshot.Executions[first.WorkflowKey])

	require.Error(t, manager.DeleteWorkflowExecution(t.Context(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID: 1, RangeID: 3, NamespaceID: first.NamespaceID, WorkflowID: first.WorkflowID,
		RunID: first.RunID, ArchetypeID: chasm.WorkflowArchetypeID,
	}))
	_, err = manager.GetWorkflowExecution(t.Context(), &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: first.NamespaceID, WorkflowID: first.WorkflowID, RunID: first.RunID, ArchetypeID: chasm.WorkflowArchetypeID})
	require.NoError(t, err)

	deleted := &tasks.ExecutionFeedTask{WorkflowKey: first.WorkflowKey, TaskID: 30, SchemaVersion: 1, Deleted: true, ArchetypeID: chasm.WorkflowArchetypeID}
	// The wrong ownership fence rolls back both deletion and its tombstone.
	deleteRequest := &persistence.DeleteWorkflowExecutionRequest{ShardID: 1, RangeID: 3,
		NamespaceID: first.NamespaceID, WorkflowID: first.WorkflowID, RunID: first.RunID, ArchetypeID: chasm.WorkflowArchetypeID, ExecutionFeedTask: deleted}
	require.Error(t, manager.DeleteWorkflowExecution(t.Context(), deleteRequest))
	_, err = manager.GetWorkflowExecution(t.Context(), &persistence.GetWorkflowExecutionRequest{
		ShardID: 1, NamespaceID: first.NamespaceID, WorkflowID: first.WorkflowID, RunID: first.RunID, ArchetypeID: chasm.WorkflowArchetypeID})
	require.NoError(t, err)
	deleteRequest.RangeID = 2
	require.NoError(t, manager.DeleteWorkflowExecution(t.Context(), deleteRequest))
	_, _, err = processor.Drain(t.Context(), testShard{high: 40}, 100)
	require.NoError(t, err)
	snapshot, err = reader.Snapshot(t.Context(), Cursor{1: 40}, 10)
	require.NoError(t, err)
	require.Empty(t, snapshot.Executions)
}
