//go:build integration

package temporal_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/executionfeed"
	"go.temporal.io/server/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestExecutionFeedHistoryRPC(t *testing.T) {
	t.Parallel()
	ctx := testcontext.For(t)
	tv := testvars.New(t)
	cfg := loadConfig(t)
	cfg.Persistence.NumHistoryShards = 1
	dc := dynamicconfig.NewMemoryClient()
	dc.OverrideSetting(dynamicconfig.NamespaceCacheRefreshInterval, 50*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.EnableExecutionFeedQueue, true)
	dc.OverrideSetting(dynamicconfig.EnableExecutionFeed, true)
	dc.OverrideSetting(dynamicconfig.TransferProcessorMaxPollInterval, 100*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.TransferProcessorUpdateAckInterval, 100*time.Millisecond)
	server, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices), temporal.WithConfig(cfg),
		temporal.WithDynamicConfigClient(dc), temporal.WithLogger(log.NewTestLogger()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Stop()) })
	require.NoError(t, server.Start())
	connect := func(service string) *grpc.ClientConn {
		connection, err := grpc.NewClient(
			fmt.Sprintf("127.0.0.1:%d", cfg.Services[service].RPC.GRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, connection.Close()) })
		return connection
	}
	frontend := workflowservice.NewWorkflowServiceClient(connect(string(primitives.FrontendService)))
	history := historyservice.NewHistoryServiceClient(connect(string(primitives.HistoryService)))
	var namespaceID, runID string
	for index, name := range []string{tv.NamespaceName().String(), tv.Any().String()} {
		_, err := frontend.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
			Namespace: name, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		})
		require.NoError(t, err)
		description, err := frontend.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: name})
		require.NoError(t, err)
		started, err := frontend.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace: name, WorkflowId: tv.WorkflowID(),
			RequestId: tv.Any().String(), WorkflowType: tv.WorkflowType(),
			TaskQueue: tv.TaskQueue(), Input: payloads.EncodeString("excluded-execution-input"),
			WorkflowTaskTimeout: durationpb.New(time.Second),
		})
		require.NoError(t, err)
		if index == 0 {
			namespaceID, runID = description.NamespaceInfo.Id, started.RunId
		}
	}
	var token []byte
	var required, epoch, reconstructionStart int64
	for {
		response, err := history.BootstrapExecutionFeed(ctx, &historyservice.BootstrapExecutionFeedRequest{
			NamespaceId: namespaceID, ShardId: 1, PageSize: 1, NextPageToken: token, CaptureEpoch: epoch,
		})
		require.NoError(t, err)
		if epoch == 0 {
			reconstructionStart = response.ReconstructionStart
			require.Positive(t, reconstructionStart)
		}
		epoch = response.CaptureEpoch
		require.Positive(t, epoch)
		token, required = response.NextPageToken, response.BootstrapCutoff
		if len(token) == 0 {
			break
		}
	}
	var through int64
	await.Require(ctx, t, func(t *await.T) {
		response, err := history.GetExecutionFeed(t.Context(), &historyservice.GetExecutionFeedRequest{
			NamespaceId: namespaceID, ShardId: 1, PageSize: 1, CaptureEpoch: epoch,
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, response.PublishedCutoff, required)
		through = response.PublishedCutoff
	}, 30*time.Second, 100*time.Millisecond)
	read := func(from, through int64) ([][]byte, []executionfeed.FeedRecord) {
		var raw [][]byte
		var records []executionfeed.FeedRecord
		for from < through {
			response, err := history.GetExecutionFeed(ctx, &historyservice.GetExecutionFeedRequest{
				NamespaceId: namespaceID, ShardId: 1, PageSize: 1, StartPosition: from, EndPosition: through, CaptureEpoch: epoch,
			})
			require.NoError(t, err)
			require.Greater(t, response.NextPosition, from)
			require.LessOrEqual(t, response.NextPosition, through)
			for _, data := range response.Records {
				require.NotContains(t, string(data), "excluded-execution-input")
				var record executionfeed.FeedRecord
				require.NoError(t, json.Unmarshal(data, &record))
				require.Equal(t, namespaceID, record.NamespaceID)
				require.Equal(t, int32(1), record.ShardID)
				raw, records = append(raw, data), append(records, record)
			}
			from = response.NextPosition
		}
		return raw, records
	}
	raw, records := read(reconstructionStart, through)
	require.NotEmpty(t, records)
	require.True(t, records[len(records)-1].Baseline)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, records[len(records)-1].State.Workflow.Status)
	_, err = frontend.TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         tv.NamespaceName().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	require.NoError(t, err)
	repeated, _ := read(reconstructionStart, through)
	require.Equal(t, raw, repeated)
	await.Require(ctx, t, func(t *await.T) {
		response, err := history.GetExecutionFeed(t.Context(), &historyservice.GetExecutionFeedRequest{
			NamespaceId: namespaceID, ShardId: 1, PageSize: 100,
			StartPosition: through, EndPosition: through, CaptureEpoch: epoch,
		})
		require.NoError(t, err)
		page, err := history.GetExecutionFeed(t.Context(), &historyservice.GetExecutionFeedRequest{
			NamespaceId: namespaceID, ShardId: 1, PageSize: 100,
			StartPosition: through, EndPosition: response.PublishedCutoff, CaptureEpoch: epoch,
		})
		require.NoError(t, err)
		require.NotEmpty(t, page.Records)
		var latest executionfeed.FeedRecord
		require.NoError(t, json.Unmarshal(page.Records[len(page.Records)-1], &latest))
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, latest.State.Workflow.Status)
	}, 30*time.Second, 100*time.Millisecond)
	persisted, err := history.GetShard(ctx, &historyservice.GetShardRequest{ShardId: 1})
	require.NoError(t, err)
	require.Equal(t, epoch, persisted.ShardInfo.ExecutionFeedCaptureEpochs[namespaceID])

	dc.OverrideSetting(dynamicconfig.EnableExecutionFeed, false)
	nextWorkflow := tv.WithWorkflowIDNumber(2)
	_, err = frontend.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace: tv.NamespaceName().String(), WorkflowId: nextWorkflow.WorkflowID(), RequestId: tv.Any().String(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
	})
	require.NoError(t, err)
	_, err = frontend.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: tv.NamespaceName().String(), WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	require.NoError(t, err)
	await.Require(ctx, t, func(t *await.T) {
		_, err := frontend.DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: tv.NamespaceName().String(), Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		})
		require.Equal(t, codes.NotFound, status.Code(err))
	}, 30*time.Second, 100*time.Millisecond)
	dc.OverrideSetting(dynamicconfig.EnableExecutionFeed, true)
	await.Require(ctx, t, func(t *await.T) {
		published, err := history.GetExecutionFeed(t.Context(), &historyservice.GetExecutionFeedRequest{
			NamespaceId: namespaceID, ShardId: 1, PageSize: 100, CaptureEpoch: epoch,
		})
		require.NoError(t, err)
		_, err = history.GetExecutionFeed(t.Context(), &historyservice.GetExecutionFeedRequest{
			NamespaceId: namespaceID, ShardId: 1, PageSize: 100, CaptureEpoch: epoch,
			StartPosition: through, EndPosition: published.PublishedCutoff,
		})
		require.ErrorContains(t, err, "capture was interrupted")
	}, 30*time.Second, 100*time.Millisecond)

	_, err = history.CloseShard(ctx, &historyservice.CloseShardRequest{ShardId: 1})
	require.NoError(t, err)
	token, required = nil, 0
	var resumedEpoch int64
	for {
		response, err := history.BootstrapExecutionFeed(ctx, &historyservice.BootstrapExecutionFeedRequest{
			NamespaceId: namespaceID, ShardId: 1, PageSize: 1, NextPageToken: token, CaptureEpoch: resumedEpoch,
		})
		require.NoError(t, err)
		if resumedEpoch == 0 {
			require.Greater(t, response.ReconstructionStart, reconstructionStart)
			reconstructionStart = response.ReconstructionStart
		}
		resumedEpoch = response.CaptureEpoch
		require.Equal(t, epoch, resumedEpoch)
		token, required = response.NextPageToken, response.BootstrapCutoff
		if len(token) == 0 {
			break
		}
	}
	await.Require(ctx, t, func(t *await.T) {
		response, err := history.GetExecutionFeed(t.Context(), &historyservice.GetExecutionFeedRequest{
			NamespaceId: namespaceID, ShardId: 1, PageSize: 1, CaptureEpoch: epoch,
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, response.PublishedCutoff, required)
		through = response.PublishedCutoff
	}, 30*time.Second, 100*time.Millisecond)
	_, fresh := read(reconstructionStart, through)
	require.NotEmpty(t, fresh)
	for _, record := range fresh {
		require.Equal(t, nextWorkflow.WorkflowID(), record.WorkflowID)
		require.Equal(t, epoch, record.CaptureEpoch)
	}
}
