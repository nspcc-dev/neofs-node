package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// Workers is an interface of worker tool.
	Workers interface {
		Start(context.Context)
		Stop()

		Add(Job Handler)
	}

	workers struct {
		cancel  context.CancelFunc
		started *int32
		wg      *sync.WaitGroup
		jobs    []Handler
	}

	// Handler is a worker's handling function.
	Handler func(ctx context.Context)

	// Jobs is a map of worker names to handlers.
	Jobs map[string]Handler

	// Job groups the parameters of worker's job.
	Job struct {
		Disabled    bool
		Immediately bool
		Timer       time.Duration
		Ticker      time.Duration
		Handler     Handler
	}
)

// New is a constructor of workers.
func New() Workers {
	return &workers{
		started: new(int32),
		wg:      new(sync.WaitGroup),
	}
}

func (w *workers) Add(job Handler) {
	w.jobs = append(w.jobs, job)
}

func (w *workers) Stop() {
	if !atomic.CompareAndSwapInt32(w.started, 1, 0) {
		// already stopped
		return
	}

	w.cancel()
	w.wg.Wait()
}

func (w *workers) Start(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(w.started, 0, 1) {
		// already started
		return
	}

	ctx, w.cancel = context.WithCancel(ctx)
	for _, job := range w.jobs {
		w.wg.Add(1)

		go func(handler Handler) {
			defer w.wg.Done()
			handler(ctx)
		}(job)
	}
}
