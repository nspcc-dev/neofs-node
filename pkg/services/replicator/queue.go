package replicator

import (
	"context"
	"errors"
	"sync"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

var errQueueFull = errors.New("replication queue is full")

// EnqueueTask submits replication task for asynchronous execution.
func (p *Replicator) EnqueueTask(task Task) error {
	select {
	case p.taskQueue <- task:
		return nil
	default:
		return errQueueFull
	}
}

// Run processes queued replication tasks until the context is canceled.
func (p *Replicator) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range defaultWorkers {
		wg.Go(func() {
			p.runWorker(ctx)
		})
	}

	wg.Wait()
}

func (p *Replicator) runWorker(ctx context.Context) {
	res := new(countingReplicationResult)
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-p.taskQueue:
			res.successes = 0
			p.HandleTask(ctx, task, res)
			if res.successes < len(task.nodes) {
				p.log.Debug("queued replication finished with missing replicas",
					zap.Stringer("object", task.addr),
					zap.Int("succeeded", res.successes),
					zap.Int("expected", len(task.nodes)),
				)
			}
		}
	}
}

type countingReplicationResult struct {
	successes int
}

func (r *countingReplicationResult) SubmitSuccessfulReplication(netmap.NodeInfo) {
	r.successes++
}
