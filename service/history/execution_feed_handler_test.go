package history

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/executionfeed"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
)

func TestEncodeExecutionFeedPage(t *testing.T) {
	t.Parallel()
	first := executionfeed.FeedRecord{ShardID: 1, ExecutionFeedTask: &tasks.ExecutionFeedTask{TaskID: 10, SchemaVersion: 1}}
	second := executionfeed.FeedRecord{ShardID: 1, ExecutionFeedTask: &tasks.ExecutionFeedTask{TaskID: 20, SchemaVersion: 1}}
	firstBytes, err := json.Marshal(first)
	require.NoError(t, err)
	secondBytes, err := json.Marshal(second)
	require.NoError(t, err)
	for _, test := range []struct {
		name    string
		records []executionfeed.FeedRecord
		limit   int
		want    [][]byte
		next    int64
	}{
		{name: "filtered empty page advances", limit: 1, next: 100},
		{name: "complete interval", records: []executionfeed.FeedRecord{first, second}, limit: 1000,
			want: [][]byte{firstBytes, secondBytes}, next: 100},
		{name: "byte bound leaves first excluded record unread", records: []executionfeed.FeedRecord{first, second},
			limit: len(firstBytes), want: [][]byte{firstBytes}, next: second.TaskID},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, next, err := encodeExecutionFeedPage(test.records, 100, test.limit)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
			require.Equal(t, test.next, next)
		})
	}
}

func TestExecutionFeedPageExactByteLimit(t *testing.T) {
	t.Parallel()
	for _, excess := range []int{0, 1} {
		t.Run(map[int]string{0: "exact limit", 1: "one byte over"}[excess], func(t *testing.T) {
			t.Parallel()
			record := executionfeed.FeedRecord{ShardID: 1, ExecutionFeedTask: &tasks.ExecutionFeedTask{
				TaskID: 10, SchemaVersion: 1, State: tasks.FeedState{Workflow: &tasks.FeedWorkflow{}},
			}}
			base, err := json.Marshal(record)
			require.NoError(t, err)
			record.State.Workflow.Type = strings.Repeat("x", executionFeedPageByteLimit-len(base)+excess)
			encoded, err := json.Marshal(record)
			require.NoError(t, err)
			require.Len(t, encoded, executionFeedPageByteLimit+excess)
			got, next, err := encodeExecutionFeedPage([]executionfeed.FeedRecord{record}, 100, executionFeedPageByteLimit)
			if excess != 0 {
				require.Error(t, err)
				var exhausted *serviceerror.ResourceExhausted
				require.ErrorAs(t, err, &exhausted)
				require.Nil(t, got)
				require.Zero(t, next)
				return
			}
			require.NoError(t, err)
			require.Equal(t, [][]byte{encoded}, got)
			require.Equal(t, int64(100), next)
		})
	}
}

func TestExecutionFeedRPCAdmission(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		queue         bool
		global        bool
		capture       bool
		captureAbsent bool
		namespace     string
		shard         int32
	}{
		{name: "queue absent", namespace: "valid", shard: 1},
		{name: "capture disabled", queue: true, namespace: "valid", shard: 1},
		{name: "capture unset", queue: true, captureAbsent: true, namespace: "valid", shard: 1},
		{name: "global namespace", queue: true, global: true, capture: true, namespace: "valid", shard: 1},
		{name: "invalid namespace", namespace: "invalid", shard: 1},
		{name: "zero shard", namespace: "valid"},
		{name: "unconfigured shard", namespace: "valid", shard: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			tv := testvars.New(t)
			registry := tasks.NewDefaultTaskCategoryRegistry()
			if test.queue {
				registry.AddCategory(tasks.CategoryExecutionFeed)
			}
			cfg := &configs.Config{NumberOfShards: 2}
			if !test.captureAbsent {
				cfg.ExecutionFeedEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(test.capture)
			}
			namespaces := namespace.NewMockRegistry(controller)
			id := test.namespace
			if id == "valid" {
				id = tv.NamespaceID().String()
			}
			if test.queue && test.shard > 0 && test.shard <= 2 && test.namespace == "valid" {
				ns := namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{
					Id: id, Name: tv.NamespaceName().String(),
				}, nil, test.global, nil, 1)
				namespaces.EXPECT().GetNamespaceByID(namespace.ID(id)).Return(ns, nil).Times(2)
			}
			handler := &Handler{config: cfg, taskCategoryRegistry: registry, namespaceRegistry: namespaces}
			response, err := handler.GetExecutionFeed(t.Context(), &historyservice.GetExecutionFeedRequest{
				NamespaceId: id, ShardId: test.shard, PageSize: 1, CaptureEpoch: 7,
			})
			require.Error(t, err)
			require.Nil(t, response)
			baseline, err := handler.BootstrapExecutionFeed(t.Context(), &historyservice.BootstrapExecutionFeedRequest{
				NamespaceId: id, ShardId: test.shard, PageSize: 1,
			})
			require.Error(t, err)
			require.Nil(t, baseline)
		})
	}
}

func TestExecutionFeedRPCRejectsMalformedBounds(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		read *historyservice.GetExecutionFeedRequest
	}{
		{name: "nil read"},
		{name: "zero read page size", read: &historyservice.GetExecutionFeedRequest{}},
		{name: "missing capture epoch", read: &historyservice.GetExecutionFeedRequest{PageSize: 1}},
		{name: "large read page size", read: &historyservice.GetExecutionFeedRequest{PageSize: 101}},
		{name: "negative position", read: &historyservice.GetExecutionFeedRequest{PageSize: 1, StartPosition: -1}},
		{name: "reversed interval", read: &historyservice.GetExecutionFeedRequest{PageSize: 1, StartPosition: 2, EndPosition: 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			response, err := (&Handler{}).GetExecutionFeed(t.Context(), test.read)
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalid)
			require.Nil(t, response)
		})
	}
	for _, test := range []struct {
		name      string
		bootstrap *historyservice.BootstrapExecutionFeedRequest
	}{
		{name: "nil bootstrap"},
		{name: "zero bootstrap page size", bootstrap: &historyservice.BootstrapExecutionFeedRequest{}},
		{name: "large bootstrap page size", bootstrap: &historyservice.BootstrapExecutionFeedRequest{PageSize: 101}},
		{name: "large token", bootstrap: &historyservice.BootstrapExecutionFeedRequest{PageSize: 1, NextPageToken: make([]byte, (16<<10)+1)}},
		{name: "continuation without capture epoch", bootstrap: &historyservice.BootstrapExecutionFeedRequest{PageSize: 1, NextPageToken: []byte("page")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			response, err := (&Handler{}).BootstrapExecutionFeed(t.Context(), test.bootstrap)
			var invalid *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalid)
			require.Nil(t, response)
		})
	}
}

type executionFeedCaptureShard struct {
	historyi.ShardContext
	historyi.ExecutionFeedCapture
}

func TestExecutionFeedRPCPublishedCutoff(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		published bool
		end       int64
		wantCode  codes.Code
	}{
		{name: "publication pending", wantCode: codes.Unavailable},
		{name: "unpublished suffix refused", published: true, end: 101, wantCode: codes.FailedPrecondition},
		{name: "namespace filtered read", published: true, end: 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			tv := testvars.New(t)
			namespaces := namespace.NewMockRegistry(controller)
			namespaces.EXPECT().GetNamespaceByID(tv.NamespaceID()).Return(tv.Namespace(), nil)
			categories := tasks.NewDefaultTaskCategoryRegistry()
			categories.AddCategory(tasks.CategoryExecutionFeed)
			owner := historyi.NewMockShardContext(controller)
			capture := historyi.NewMockExecutionFeedCapture(controller)
			capture.EXPECT().ExecutionFeedCaptureEpoch(tv.NamespaceID()).Return(int64(7))
			owner.EXPECT().GetEngine(gomock.Any()).Return(historyi.NewMockEngine(controller), nil)
			owner.EXPECT().GetQueueState(tasks.CategoryExecutionFeed).Return(
				&persistencespb.QueueState{ExclusiveReaderHighWatermark: &persistencespb.TaskKey{TaskId: 100}}, test.published)
			shards := shard.NewMockController(controller)
			shards.EXPECT().GetShardByID(int32(1)).Return(&executionFeedCaptureShard{owner, capture}, nil)
			handler := &Handler{
				config:               &configs.Config{NumberOfShards: 1, ExecutionFeedEnabled: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)},
				taskCategoryRegistry: categories, namespaceRegistry: namespaces, controller: shards,
			}
			record := &tasks.ExecutionFeedTask{SchemaVersion: 2, CaptureEpoch: 7, TaskID: 20,
				WorkflowKey: definition.NewWorkflowKey(tv.NamespaceID().String(), tv.WorkflowID(), tv.RunID())}
			if test.wantCode == codes.OK {
				manager := persistence.NewMockExecutionManager(controller)
				owner.EXPECT().GetExecutionManager().Return(manager)
				manager.EXPECT().GetHistoryTasks(gomock.Any(), &persistence.GetHistoryTasksRequest{
					ShardID: 1, TaskCategory: tasks.CategoryExecutionFeedJournal, BatchSize: 3,
					InclusiveMinTaskKey: tasks.NewImmediateKey(0), ExclusiveMaxTaskKey: tasks.NewImmediateKey(100),
				}).Return(&persistence.GetHistoryTasksResponse{Tasks: []tasks.Task{
					&tasks.ExecutionFeedTask{SchemaVersion: 1, TaskID: 10, WorkflowKey: definition.NewWorkflowKey("other-namespace", tv.WorkflowID(), tv.RunID())},
					record,
				}}, nil)
			}
			response, err := handler.GetExecutionFeed(t.Context(), &historyservice.GetExecutionFeedRequest{
				NamespaceId: tv.NamespaceID().String(), ShardId: 1, PageSize: 3, EndPosition: test.end, CaptureEpoch: 7,
			})
			if test.wantCode != codes.OK {
				require.Equal(t, test.wantCode, serviceerror.ToStatus(err).Code())
				require.Nil(t, response)
				return
			}
			require.NoError(t, err)
			expected, err := json.Marshal(executionfeed.FeedRecord{ShardID: 1, ExecutionFeedTask: record})
			require.NoError(t, err)
			require.Equal(t, &historyservice.GetExecutionFeedResponse{
				Records: [][]byte{expected}, NextPosition: 100, PublishedCutoff: 100,
			}, response)
		})
	}
}
