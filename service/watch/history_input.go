package watch

import (
	"context"
	"encoding/base64"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/executionfeed"
	"go.temporal.io/server/service/watch/native"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type TemporalInput interface{ temporalInput() }

type ManualTemporalInput struct{}

type HistoryTemporalInput struct {
	Client     historyFeedClient
	ShardCount int32
}

func (ManualTemporalInput) temporalInput()  {}
func (HistoryTemporalInput) temporalInput() {}

type historyFeedClient interface {
	BootstrapExecutionFeed(context.Context, *historyservice.BootstrapExecutionFeedRequest, ...grpc.CallOption) (*historyservice.BootstrapExecutionFeedResponse, error)
	GetExecutionFeed(context.Context, *historyservice.GetExecutionFeedRequest, ...grpc.CallOption) (*historyservice.GetExecutionFeedResponse, error)
}

type historyRuntime interface {
	Apply(context.Context, native.Command) (native.Result, error)
}

type historyInputWorker struct {
	runtime historyRuntime
	client  historyFeedClient
	source  string
	scope   native.HistoryScope
	cancel  context.CancelFunc
	done    chan struct{}
	problem string
}

func (worker *historyInputWorker) run(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		caughtUp := true
		for shard := int32(1); shard <= int32(worker.scope.ShardCount); shard++ {
			ready, err := worker.advance(ctx, shard)
			if err != nil {
				return err
			}
			caughtUp = caughtUp && ready
		}
		if !caughtUp {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (worker *historyInputWorker) advance(ctx context.Context, shard int32) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := worker.runtime.Apply(ctx, native.HistoryProgressCommand{SourceID: worker.source, ShardID: uint32(shard)})
	if err != nil {
		return false, err
	}
	progress := result.HistoryProgress
	if progress == nil {
		return false, serviceerror.NewDataLoss("native History progress is missing")
	}
	step := native.HistoryStep{RequestID: uuid.NewString(), ShardID: uint32(shard), ExpectedRevision: progress.Revision}
	cursor := progress.Cursor
	if cursor.Phase == native.HistoryUnstarted ||
		(cursor.Phase == native.HistoryCapturing && cursor.Scan != nil && cursor.Scan.Phase == native.HistoryScanning) {
		page, err := worker.bootstrapPage(ctx, shard, cursor)
		if err != nil {
			return false, err
		}
		step.Page = page
		_, err = worker.accept(ctx, step)
		return false, err
	}
	if cursor.Phase != native.HistoryCapturing || cursor.Scan == nil || cursor.Scan.Phase != native.HistoryScanComplete || cursor.CaptureEpoch <= 0 ||
		cursor.ReconstructionStart <= 0 || cursor.NextPosition < cursor.ReconstructionStart {
		return false, serviceerror.NewDataLoss("native History capture cursor is invalid")
	}
	page, err := worker.readPage(ctx, shard, cursor)
	if err != nil {
		return false, err
	}
	for {
		step.Page = page
		size, err := native.CommandSize(native.AcceptHistoryCommand{SourceID: worker.source, Step: step})
		if err != nil {
			return false, err
		}
		if size <= native.MaxCommandBytes {
			break
		}
		if len(page.Records) <= 1 {
			return false, serviceerror.NewInvalidArgument("one History projection exceeds the native input byte limit")
		}
		keep := len(page.Records) / 2
		page.NextPosition = page.Records[keep].Position
		page.Records = page.Records[:keep]
	}
	_, err = worker.accept(ctx, step)
	return page.NextPosition == page.PublishedCutoff && page.NextPosition >= cursor.Scan.Cutoff, err
}

func (worker *historyInputWorker) bootstrapPage(ctx context.Context, shard int32, cursor native.HistoryCursor) (native.HistoryBootstrapPage, error) {
	var token []byte
	if cursor.Scan != nil {
		var err error
		token, err = base64.StdEncoding.Strict().DecodeString(cursor.Scan.Token)
		if err != nil || len(token) > 16<<10 {
			return native.HistoryBootstrapPage{}, serviceerror.NewDataLoss("native History bootstrap token is invalid")
		}
	}
	response, err := worker.client.BootstrapExecutionFeed(ctx, &historyservice.BootstrapExecutionFeedRequest{
		NamespaceId: worker.scope.NamespaceID, ShardId: shard, PageSize: 100,
		NextPageToken: token, CaptureEpoch: cursor.CaptureEpoch,
	})
	if err != nil {
		return native.HistoryBootstrapPage{}, err
	}
	if response == nil || len(response.NextPageToken) > 16<<10 {
		return native.HistoryBootstrapPage{}, serviceerror.NewDataLoss("History bootstrap returned an invalid response")
	}
	start := cursor.ReconstructionStart
	if cursor.Phase == native.HistoryUnstarted {
		start = response.ReconstructionStart
	} else if response.ReconstructionStart != 0 && response.ReconstructionStart != start {
		return native.HistoryBootstrapPage{}, executionfeed.CaptureDiscontinuity("History reconstruction start changed during bootstrap")
	}
	return native.HistoryBootstrapPage{
		CaptureEpoch: response.CaptureEpoch, ReconstructionStart: start,
		NextToken: base64.StdEncoding.EncodeToString(response.NextPageToken), Cutoff: response.BootstrapCutoff,
	}, nil
}

func (worker *historyInputWorker) readPage(ctx context.Context, shard int32, cursor native.HistoryCursor) (native.HistoryRecordsPage, error) {
	request := &historyservice.GetExecutionFeedRequest{
		NamespaceId: worker.scope.NamespaceID, ShardId: shard, PageSize: 100,
		CaptureEpoch: cursor.CaptureEpoch, StartPosition: cursor.NextPosition, EndPosition: cursor.NextPosition,
	}
	response, err := worker.client.GetExecutionFeed(ctx, request)
	if err != nil {
		return native.HistoryRecordsPage{}, err
	}
	if response == nil || len(response.Records) != 0 || response.NextPosition != cursor.NextPosition ||
		response.PublishedCutoff < cursor.NextPosition {
		return native.HistoryRecordsPage{}, serviceerror.NewDataLoss("History cutoff discovery returned an invalid response")
	}
	if response.PublishedCutoff > cursor.NextPosition {
		request.EndPosition = response.PublishedCutoff
		response, err = worker.client.GetExecutionFeed(ctx, request)
		if err != nil {
			return native.HistoryRecordsPage{}, err
		}
	}
	if response == nil || len(response.Records) > 100 || response.NextPosition < request.StartPosition ||
		(request.EndPosition > request.StartPosition && response.NextPosition == request.StartPosition) ||
		response.NextPosition > request.EndPosition || response.PublishedCutoff < request.EndPosition {
		return native.HistoryRecordsPage{}, serviceerror.NewDataLoss("History read returned an invalid page")
	}
	page := native.HistoryRecordsPage{
		CaptureEpoch: cursor.CaptureEpoch, StartPosition: cursor.NextPosition,
		NextPosition: response.NextPosition, PublishedCutoff: response.PublishedCutoff,
		Records: make([]native.HistoryRecord, 0, len(response.Records)),
	}
	bytes := 0
	for _, payload := range response.Records {
		bytes += len(payload)
		if bytes > 3<<20 {
			return native.HistoryRecordsPage{}, serviceerror.NewDataLoss("History read exceeds the response byte limit")
		}
		record, err := historyRecord(worker.source, worker.scope, shard, cursor.CaptureEpoch, payload)
		if err != nil {
			return native.HistoryRecordsPage{}, err
		}
		if record.Position < page.StartPosition || record.Position >= page.NextPosition ||
			(len(page.Records) != 0 && record.Position <= page.Records[len(page.Records)-1].Position) {
			return native.HistoryRecordsPage{}, serviceerror.NewDataLoss("History projection positions are outside the ordered page")
		}
		page.Records = append(page.Records, record)
	}
	return page, nil
}

func (g *Graph) temporalRecipe() native.NodeRecipe {
	switch input := g.options.TemporalInput.(type) {
	case ManualTemporalInput:
		return native.TemporalSourceRecipe{}
	case HistoryTemporalInput:
		return native.HistorySourceRecipe{Scope: native.HistoryScope{
			NamespaceID: g.options.NamespaceID, ShardCount: uint32(input.ShardCount),
		}}
	default:
		return nil
	}
}

func (g *Graph) startHistoryWorkersLocked() {
	input, ok := g.options.TemporalInput.(HistoryTemporalInput)
	if !ok {
		return
	}
	for id, node := range g.nodes {
		recipe, history := node.Definition.Recipe.(native.HistorySourceRecipe)
		if !history || node.HistoryBlocked || !nativeNodePermitted(g.view, node, g.nodes) ||
			(node.Status != native.NodeStarting && node.Status != native.NodeRunning) {
			continue
		}
		if _, retained := g.history[id]; retained {
			continue
		}
		ctx, cancel := context.WithCancel(g.runContext)
		worker := &historyInputWorker{
			runtime: g.runtime, client: input.Client, source: id, scope: recipe.Scope,
			cancel: cancel, done: make(chan struct{}),
		}
		g.history[id] = worker
		g.workers.Add(1)
		go g.runHistoryWorker(ctx, worker)
	}
}

func (g *Graph) runHistoryWorker(ctx context.Context, worker *historyInputWorker) {
	defer g.workers.Done()
	defer close(worker.done)
	err := worker.run(ctx)
	g.mu.Lock()
	if err != nil && ctx.Err() == nil {
		worker.problem = err.Error()
		g.applyHistoryFailuresLocked()
		g.observed = false
	}
	g.mu.Unlock()
	if ctx.Err() == nil {
		var command native.Command = native.SuspendHistoryCommand{SourceID: worker.source}
		if permanentHistoryFailure(err) {
			command = native.BlockHistoryCommand{SourceID: worker.source, Problem: "History continuity or projection validation failed; a new source incarnation is required"}
		}
		_, suspendErr := g.runtime.Apply(ctx, command)
		err = errors.Join(err, suspendErr)
	}
	g.mu.Lock()
	if err != nil && ctx.Err() == nil {
		worker.problem = err.Error()
	}
	g.mu.Unlock()
}

func (g *Graph) applyHistoryFailuresLocked() {
	var detached bool
	for id, worker := range g.history {
		if worker.problem == "" {
			continue
		}
		if node, ok := g.nodes[id]; ok && (node.Status == native.NodeStarting || node.Status == native.NodeRunning) {
			if !detached {
				g.nodes = maps.Clone(g.nodes)
				detached = true
			}
			node.Status = native.NodeFailed
			g.nodes[id] = node
		}
		for _, watch := range g.watches {
			if watch.binding == nil || !watch.accepting {
				continue
			}
			query := g.nodes[watch.binding.Definition.QueryID]
			recipe, ok := query.Definition.Recipe.(native.QueryRecipe)
			if !ok || !slices.Contains(recipe.Sources, id) {
				continue
			}
			watch.accepting, watch.quiesced = false, false
			if watch.cancel != nil {
				watch.cancel()
			}
		}
	}
}

func (g *Graph) stopHistoryWorker(ctx context.Context, id string) error {
	g.mu.Lock()
	worker, ok := g.history[id]
	if ok {
		worker.cancel()
	}
	g.mu.Unlock()
	if !ok {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-worker.done:
	}
	return nil
}

func (worker *historyInputWorker) accept(ctx context.Context, step native.HistoryStep) (*native.HistoryReceipt, error) {
	result, err := worker.runtime.Apply(ctx, native.AcceptHistoryCommand{SourceID: worker.source, Step: step})
	if err == nil {
		receipt := result.HistoryReceipt
		if receipt == nil || receipt.RequestID != step.RequestID || receipt.Revision != step.ExpectedRevision+1 {
			return nil, serviceerror.NewDataLoss("native History acceptance returned an invalid receipt")
		}
		return receipt, nil
	}
	if ctx.Err() != nil {
		return nil, err
	}
	observed, readErr := worker.runtime.Apply(ctx, native.HistoryProgressCommand{SourceID: worker.source, ShardID: step.ShardID})
	if readErr != nil {
		return nil, errors.Join(err, readErr)
	}
	if progress := observed.HistoryProgress; progress != nil && progress.Revision == step.ExpectedRevision+1 &&
		progress.Receipt != nil && progress.Receipt.RequestID == step.RequestID && progress.Receipt.Revision == progress.Revision {
		return progress.Receipt, nil
	}
	return nil, err
}

func permanentHistoryFailure(err error) bool {
	rpc := serviceerror.ToStatus(err)
	if rpc.Code() == codes.DataLoss || rpc.Code() == codes.InvalidArgument {
		return true
	}
	for _, detail := range rpc.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok &&
			info.Domain == "temporal.execution_feed" && info.Reason == executionfeed.CaptureGapReason {
			return true
		}
	}
	return false
}
