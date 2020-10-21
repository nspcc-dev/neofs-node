package policer

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

// Task represents group of Policer tact parameters.
type Task struct{}

type prevTask struct {
	undone int

	cancel context.CancelFunc

	wait *sync.WaitGroup
}

type workScope struct {
	val int

	expRate int // in %
}

func (p *Policer) Run(ctx context.Context) {
	defer func() {
		p.log.Info("routine stopped")
	}()

	p.log.Info("process routine",
		zap.Int("work scope value", p.workScope.val),
		zap.Int("expansion rate (%)", p.workScope.val),
		zap.Duration("head timeout", p.headTimeout),
	)

	for {
		select {
		case <-ctx.Done():
			p.prevTask.cancel()

			p.log.Warn("context is done",
				zap.String("error", ctx.Err().Error()),
			)

			return
		case task, ok := <-p.trigger:
			if !ok {
				p.log.Warn("trigger channel is closed")

				return
			}

			p.prevTask.cancel()
			p.prevTask.wait.Wait()

			var taskCtx context.Context

			taskCtx, p.prevTask.cancel = context.WithCancel(ctx)

			go p.handleTask(taskCtx, task)
		}
	}
}

func (p *Policer) handleTask(ctx context.Context, task *Task) {
	p.prevTask.wait.Add(1)

	defer func() {
		p.prevTask.wait.Done()
		p.log.Info("finish work",
			zap.Int("amount of unfinished objects", p.prevTask.undone),
		)
	}()

	var delta int

	// undone - amount of objects we couldn't process in last epoch
	if p.prevTask.undone > 0 {
		// if there are unprocessed objects, then lower your estimation
		delta = -p.prevTask.undone
	} else {
		// otherwise try to expand
		delta = p.workScope.val * p.workScope.expRate / 100
	}

	addrs, err := p.jobQueue.Select(p.workScope.val + delta)
	if err != nil {
		p.log.Warn("could not select objects",
			zap.String("error", err.Error()),
		)
	}

	// if there are NOT enough objects to fill the pool, do not change it
	// otherwise expand or shrink it with the delta value
	if len(addrs) >= p.workScope.val+delta {
		p.workScope.val += delta
	}

	p.prevTask.undone = len(addrs)

	for i := range addrs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		p.processObject(ctx, addrs[i])

		p.prevTask.undone--
	}
}
