package executionfeed

import (
	"context"
	"sync"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log/tag"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

// Queue ingests serially per shard, independently of Watch/evaluator availability.
type Queue struct {
	shard  historyi.ShardContext
	wake   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	start  sync.Once
	stop   sync.Once
}

func NewQueue(shard historyi.ShardContext) *Queue {
	ctx, cancel := context.WithCancel(shard.GetLifecycleContext())
	return &Queue{shard: shard, wake: make(chan struct{}, 1), ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

func (*Queue) Category() tasks.Category { return tasks.CategoryExecutionFeed }
func (*Queue) FailoverNamespace(string) {}
func (q *Queue) NotifyNewTasks([]tasks.Task) {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
func (q *Queue) Start() { q.start.Do(func() { go q.run() }) }
func (q *Queue) Stop() {
	q.stop.Do(func() {
		q.cancel()
		q.Start()
		<-q.done
	})
}

func (q *Queue) run() {
	defer close(q.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	p := Processor{Store: q.shard.GetExecutionManager()}
	for {
		select {
		case <-q.ctx.Done():
			return
		case <-q.wake:
		case <-ticker.C:
		}
		for q.ctx.Err() == nil {
			cut, caughtUp, err := p.Drain(q.ctx, q.shard, 100)
			if err != nil {
				q.shard.GetLogger().Error("Execution feed journal ingestion failed", tag.Error(err))
				break
			}
			if caughtUp {
				err := q.shard.SetQueueState(tasks.CategoryExecutionFeed, 0, &persistencespb.QueueState{
					ExclusiveReaderHighWatermark: &persistencespb.TaskKey{TaskId: cut},
				})
				if err != nil {
					q.shard.GetLogger().Error("Execution feed cutoff publication failed", tag.Error(err))
				}
				break
			}
		}
	}
}

// PublishedCut is a durable ingestion cutoff, not a highest-observed task ID.
// Bootstrap completion must be tracked separately for each source incarnation.
func PublishedCut(shard historyi.ShardContext) (int64, bool) {
	state, ok := shard.GetQueueState(tasks.CategoryExecutionFeed)
	if !ok || state.ExclusiveReaderHighWatermark == nil {
		return 0, false
	}
	return state.ExclusiveReaderHighWatermark.TaskId, true
}
