package replicator

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

// EnqueueTask submits replication task for asynchronous execution and blocks
// until the task is accepted by the queue or the context is canceled.
func (p *Replicator) EnqueueTask(ctx context.Context, task Task) error {
	select {
	case p.taskQueue <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run processes queued replication tasks until the context is canceled.
func (p *Replicator) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range p.workers {
		wg.Go(func() {
			p.runWorker(ctx)
		})
	}

	<-ctx.Done()
	wg.Wait()
}

func (p *Replicator) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-p.taskQueue:
			res := new(countingReplicationResult)
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
