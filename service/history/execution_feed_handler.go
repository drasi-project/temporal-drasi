package history

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/executionfeed"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

const executionFeedPageByteLimit = 3 << 20

func (h *Handler) BootstrapExecutionFeed(
	ctx context.Context,
	request *historyservice.BootstrapExecutionFeedRequest,
) (*historyservice.BootstrapExecutionFeedResponse, error) {
	if request.GetPageSize() < 1 || request.GetPageSize() > 100 || len(request.GetNextPageToken()) > 16<<10 ||
		request.GetCaptureEpoch() < 0 || (request.GetCaptureEpoch() == 0 && len(request.GetNextPageToken()) > 0) {
		return nil, serviceerror.NewInvalidArgument("execution feed bootstrap requires a page size between 1 and 100, a token of at most 16 KiB, and a nonnegative capture epoch; continuation requires an established epoch")
	}
	shard, err := h.executionFeedShard(ctx, request.NamespaceId, request.ShardId)
	if err != nil {
		return nil, h.convertError(err)
	}
	capture, ok := shard.(historyi.ExecutionFeedCapture)
	if !ok {
		return nil, serviceerror.NewFailedPrecondition("owning shard does not support durable execution feed capture")
	}
	scope := &historyi.ExecutionFeedCaptureScope{Epoch: request.CaptureEpoch}
	if scope.Epoch == 0 {
		scope, err = capture.BeginExecutionFeedCapture(ctx, namespace.ID(request.NamespaceId))
		if err != nil {
			return nil, h.convertError(err)
		}
	} else if scope.Epoch != capture.ExecutionFeedCaptureEpoch(namespace.ID(request.NamespaceId)) {
		return nil, executionfeed.CaptureDiscontinuity("execution feed capture session has changed")
	}
	next, err := executionfeed.BootstrapPage(ctx, shard, request.NamespaceId, request.NextPageToken, int(request.PageSize))
	if err != nil {
		return nil, h.convertError(err)
	}
	return &historyservice.BootstrapExecutionFeedResponse{
		NextPageToken:   next,
		BootstrapCutoff: shard.GetQueueExclusiveHighReadWatermark(tasks.CategoryExecutionFeed).TaskID,
		CaptureEpoch:    scope.Epoch, ReconstructionStart: scope.ReconstructionStart,
	}, nil
}

func (h *Handler) GetExecutionFeed(
	ctx context.Context,
	request *historyservice.GetExecutionFeedRequest,
) (*historyservice.GetExecutionFeedResponse, error) {
	if request.GetPageSize() < 1 || request.GetPageSize() > 100 ||
		request.GetStartPosition() < 0 || request.GetEndPosition() < request.GetStartPosition() ||
		request.GetCaptureEpoch() <= 0 {
		return nil, serviceerror.NewInvalidArgument("execution feed read requires an ordered nonnegative interval, a page size between 1 and 100, and a positive capture epoch")
	}
	shard, err := h.executionFeedShard(ctx, request.NamespaceId, request.ShardId)
	if err != nil {
		return nil, h.convertError(err)
	}
	capture, ok := shard.(historyi.ExecutionFeedCapture)
	if !ok || capture.ExecutionFeedCaptureEpoch(namespace.ID(request.NamespaceId)) != request.CaptureEpoch {
		return nil, executionfeed.CaptureDiscontinuity("execution feed capture session has changed")
	}
	published, ok := executionfeed.PublishedCut(shard)
	if !ok {
		return nil, serviceerror.NewUnavailable("execution feed ingestion has not published a durable cutoff")
	}
	if request.EndPosition > published {
		return nil, serviceerror.NewFailedPrecondition("execution feed read exceeds the durable published cutoff")
	}
	reader := executionfeed.Reader{Store: shard.GetExecutionManager(), NamespaceID: request.NamespaceId, CaptureEpoch: request.CaptureEpoch}
	records, next, err := reader.Read(ctx, request.ShardId, request.StartPosition, request.EndPosition, int(request.PageSize))
	if err != nil {
		return nil, h.convertError(err)
	}
	encoded, next, err := encodeExecutionFeedPage(records, next, executionFeedPageByteLimit)
	if err != nil {
		return nil, err
	}
	return &historyservice.GetExecutionFeedResponse{
		Records: encoded, NextPosition: next, PublishedCutoff: published,
	}, nil
}

func (h *Handler) executionFeedShard(ctx context.Context, namespaceID string, shardID int32) (historyi.ShardContext, error) {
	id, err := uuid.Parse(namespaceID)
	if err != nil || id == uuid.Nil || id.String() != namespaceID || shardID < 1 || shardID > h.config.NumberOfShards {
		return nil, serviceerror.NewInvalidArgument("execution feed requires a canonical namespace UUID and a configured positive shard ID")
	}
	if _, ok := h.taskCategoryRegistry.GetCategoryByID(tasks.CategoryExecutionFeed.ID()); !ok {
		return nil, serviceerror.NewFailedPrecondition("execution feed ingestion queue is not registered")
	}
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return nil, err
	}
	if ns.IsGlobalNamespace() {
		return nil, serviceerror.NewFailedPrecondition("execution feed does not support global namespaces")
	}
	if h.config.ExecutionFeedEnabled == nil || !h.config.ExecutionFeedEnabled(ns.Name().String()) {
		return nil, serviceerror.NewFailedPrecondition("execution feed capture is disabled")
	}
	shard, err := h.controller.GetShardByID(shardID)
	if err != nil {
		return nil, err
	}
	if _, err := shard.GetEngine(ctx); err != nil {
		return nil, err
	}
	return shard, nil
}

func encodeExecutionFeedPage(records []executionfeed.FeedRecord, next int64, limit int) ([][]byte, int64, error) {
	var encoded [][]byte
	size := 0
	for _, record := range records {
		data, err := json.Marshal(record)
		if err != nil {
			return nil, 0, err
		}
		if len(data) > limit {
			return nil, 0, serviceerror.NewResourceExhausted(
				enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, "execution feed record exceeds the response byte limit")
		}
		if size+len(data) > limit {
			return encoded, record.TaskID, nil
		}
		encoded = append(encoded, data)
		size += len(data)
	}
	return encoded, next, nil
}
