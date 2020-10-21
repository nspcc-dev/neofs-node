package main

import (
	"context"
)

type worker interface {
	Run(context.Context)
}

type workerFromFunc struct {
	fn func(context.Context)
}

func newWorkerFromFunc(fn func(ctx context.Context)) worker {
	return &workerFromFunc{
		fn: fn,
	}
}

func (w *workerFromFunc) Run(ctx context.Context) {
	w.fn(ctx)
}

func startWorkers(c *cfg) {
	for _, wrk := range c.workers {
		c.wg.Add(1)

		go func(w worker) {
			w.Run(c.ctx)
			c.wg.Done()
		}(wrk)
	}
}
