package shard

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func captureTestShard(t *testing.T) *ContextTest {
	t.Helper()
	s := NewTestContext(gomock.NewController(t), &persistencespb.ShardInfo{ShardId: 1, RangeId: 1}, tests.NewDynamicConfig())
	t.Cleanup(s.FinishStop)
	s.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	return s
}

func TestExecutionFeedCaptureActivationAndFreshReconstruction(t *testing.T) {
	t.Parallel()
	s := captureTestShard(t)
	var stored *persistencespb.ShardInfo
	s.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateShardRequest) error {
			stored = common.CloneProto(request.ShardInfo)
			return nil
		}).Times(2)
	first, err := s.BeginExecutionFeedCapture(t.Context(), tests.NamespaceID)
	require.NoError(t, err)
	require.Equal(t, &historyi.ExecutionFeedCaptureScope{
		Epoch: 2, ReconstructionStart: 2 << s.config.RangeSizeBits,
	}, first)
	require.Equal(t, map[string]int64{tests.NamespaceID.String(): 2}, stored.ExecutionFeedCaptureEpochs)
	stored.ExecutionFeedCaptureEpochs[tests.NamespaceID.String()] = 99
	require.Equal(t, int64(2), s.ExecutionFeedCaptureEpoch(tests.NamespaceID))
	second, err := s.BeginExecutionFeedCapture(t.Context(), tests.NamespaceID)
	require.NoError(t, err)
	require.Equal(t, &historyi.ExecutionFeedCaptureScope{
		Epoch: first.Epoch, ReconstructionStart: 3 << s.config.RangeSizeBits,
	}, second)
	require.Equal(t, map[string]int64{tests.NamespaceID.String(): 2}, stored.ExecutionFeedCaptureEpochs)
}

func TestExecutionFeedCaptureFailedActivationDoesNotCertifyScope(t *testing.T) {
	t.Parallel()
	s := captureTestShard(t)
	s.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(&persistence.ConditionFailedError{Msg: "activation not committed"})
	scope, err := s.BeginExecutionFeedCapture(t.Context(), tests.NamespaceID)
	require.ErrorContains(t, err, "activation not committed")
	require.Nil(t, scope)
	require.Zero(t, s.ExecutionFeedCaptureEpoch(tests.NamespaceID))
	require.Equal(t, int64(1), s.GetRangeID())
}

func TestExecutionFeedCaptureWaitsForEmptyTaskWriteThenFencesUncertainty(t *testing.T) {
	t.Parallel()
	s := captureTestShard(t)
	s.ioSemaphore = locks.NewPrioritySemaphore(2)
	submitted, finish := make(chan int64, 1), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-finish:
		default:
			close(finish)
		}
	})
	writeResult := make(chan error, 1)
	s.Resource.ExecutionMgr.EXPECT().CreateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
			submitted <- request.RangeID
			select {
			case <-finish:
				return nil, serviceerror.NewUnavailable("unknown primary commit outcome")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	s.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	go func() {
		_, err := s.CreateWorkflowExecution(t.Context(), &persistence.CreateWorkflowExecutionRequest{
			NewWorkflowSnapshot: persistence.WorkflowSnapshot{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: tests.NamespaceID.String(), WorkflowId: tests.WorkflowID,
			}},
		})
		writeResult <- err
	}()
	require.Equal(t, int64(1), await.Rcv(t, submitted))
	type result struct {
		scope *historyi.ExecutionFeedCaptureScope
		err   error
	}
	activation := make(chan result, 1)
	go func() {
		scope, err := s.BeginExecutionFeedCapture(t.Context(), tests.NamespaceID)
		activation <- result{scope, err}
	}()
	await.Require(t.Context(), t, func(t *await.T) {
		s.taskKeyManager.tracker.Lock()
		waiters := len(s.taskKeyManager.tracker.waitChannels)
		s.taskKeyManager.tracker.Unlock()
		require.Equal(t, 1, waiters)
	}, 5*time.Second, time.Millisecond)
	close(finish)
	activated := await.Rcv(t, activation)
	require.NoError(t, activated.err)
	require.Equal(t, &historyi.ExecutionFeedCaptureScope{
		Epoch: 2, ReconstructionStart: 2 << s.config.RangeSizeBits,
	}, activated.scope)
	require.ErrorContains(t, await.Rcv(t, writeResult), "unknown primary commit outcome")
	require.Equal(t, contextStateAcquired, s.state)
}

func TestExecutionFeedCaptureRejectsPreparedOldTransactions(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"create", "update", "continue as new", "reset", "reset with current", "reset with new", "set"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			s := captureTestShard(t)
			s.shardInfo.ExecutionFeedCaptureEpochs = map[string]int64{tests.NamespaceID.String(): 7}
			info := &persistencespb.WorkflowExecutionInfo{NamespaceId: tests.NamespaceID.String(), WorkflowId: tests.WorkflowID}
			snapshot := persistence.WorkflowSnapshot{ExecutionInfo: info}
			mutation := persistence.WorkflowMutation{ExecutionInfo: info, ExecutionFeedCaptureEpoch: 7}
			var err error
			switch operation {
			case "create":
				_, err = s.CreateWorkflowExecution(t.Context(), &persistence.CreateWorkflowExecutionRequest{NewWorkflowSnapshot: snapshot})
			case "update":
				mutation.ExecutionFeedCaptureEpoch = 0
				_, err = s.UpdateWorkflowExecution(t.Context(), &persistence.UpdateWorkflowExecutionRequest{UpdateWorkflowMutation: mutation})
			case "continue as new":
				_, err = s.UpdateWorkflowExecution(t.Context(), &persistence.UpdateWorkflowExecutionRequest{
					UpdateWorkflowMutation: mutation, NewWorkflowSnapshot: &snapshot,
				})
			case "reset", "reset with current", "reset with new":
				request := &persistence.ConflictResolveWorkflowExecutionRequest{ResetWorkflowSnapshot: snapshot}
				if operation != "reset" {
					request.ResetWorkflowSnapshot.ExecutionFeedCaptureEpoch = 7
				}
				if operation == "reset with current" {
					mutation.ExecutionFeedCaptureEpoch = 0
					request.CurrentWorkflowMutation = &mutation
				}
				if operation == "reset with new" {
					request.NewWorkflowSnapshot = &snapshot
				}
				_, err = s.ConflictResolveWorkflowExecution(t.Context(), request)
			case "set":
				_, err = s.SetWorkflowExecution(t.Context(), &persistence.SetWorkflowExecutionRequest{SetWorkflowSnapshot: snapshot})
			default:
				t.Fatalf("unhandled mutation operation %q", operation)
			}
			_, ok := errors.AsType[*persistence.WorkflowConditionFailedError](err)
			require.True(t, ok, "%v", err)
			require.Equal(t, int64(1)<<s.config.RangeSizeBits, s.taskKeyManager.peekTaskKey(tasks.CategoryExecutionFeed).TaskID)
		})
	}
}

func TestExecutionFeedCaptureNamespaceLimit(t *testing.T) {
	t.Parallel()
	s := captureTestShard(t)
	s.shardInfo.ExecutionFeedCaptureEpochs = make(map[string]int64)
	for i := range 1024 {
		s.shardInfo.ExecutionFeedCaptureEpochs[fmt.Sprint(i)] = 1
	}
	scope, err := s.BeginExecutionFeedCapture(t.Context(), tests.NamespaceID)
	_, exhausted := errors.AsType[*serviceerror.ResourceExhausted](err)
	require.True(t, exhausted)
	require.Nil(t, scope)
	require.Len(t, s.shardInfo.ExecutionFeedCaptureEpochs, 1024)
	require.Equal(t, int64(1), s.GetRangeID())
}

func TestExecutionFeedCaptureGapDeletionWithoutRegisteredQueue(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)
	s := captureTestShard(t)
	tv := testvars.New(t)
	s.config.ExecutionFeedEnabled = func(string) bool { return false }
	s.shardInfo.ExecutionFeedCaptureEpochs = map[string]int64{tests.NamespaceID.String(): 7}
	var actual *tasks.ExecutionFeedTask
	s.Resource.ExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.DeleteWorkflowExecutionRequest) error {
			actual = request.ExecutionFeedTask
			return nil
		})
	engine := historyi.NewMockEngine(controller)
	engine.EXPECT().NotifyNewTasks(gomock.Any())
	engine.EXPECT().Stop()
	s.SetEngineForTesting(engine)
	request := &persistence.DeleteWorkflowExecutionRequest{NamespaceID: tests.NamespaceID.String(),
		WorkflowID: tv.WorkflowID(), RunID: tv.RunID()}
	require.NoError(t, s.deleteWorkflowExecutionWithFeed(t.Context(), request, tests.LocalNamespaceEntry, 1))
	require.Equal(t, &tasks.ExecutionFeedTask{
		WorkflowKey: definition.NewWorkflowKey(tests.NamespaceID.String(), tv.WorkflowID(), tv.RunID()),
		TaskID:      1 << s.config.RangeSizeBits, SchemaVersion: 2, CaptureEpoch: 7, CaptureGap: true,
		Version: 1, VisibilityTimestamp: actual.VisibilityTimestamp,
	}, actual)
}

func TestExecutionFeedDeletionGate(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                         string
		registered, namespaceEnabled bool
		namespace                    *namespace.Namespace
		want                         bool
	}{
		{name: "off", namespace: tests.LocalNamespaceEntry},
		{name: "off with removed namespace"},
		{name: "enabled", registered: true, namespaceEnabled: true, namespace: tests.LocalNamespaceEntry, want: true},
		{name: "namespace excluded", registered: true, namespace: tests.LocalNamespaceEntry},
		{name: "removed namespace retains tombstones", registered: true, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			registry := tasks.NewDefaultTaskCategoryRegistry()
			if test.registered {
				registry.AddCategory(tasks.CategoryExecutionFeed)
			}
			s := &ContextImpl{taskCategoryRegistry: registry, config: &configs.Config{
				ExecutionFeedEnabled: func(string) bool { return test.namespaceEnabled },
			}}
			require.Equal(t, test.want, s.executionFeedDeletionEnabled(test.namespace))
		})
	}
}
