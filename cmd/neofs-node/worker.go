package main

import (
	"context"
)

type worker struct {
	name string
	fn   func(context.Context)
}

func newWorkerFromFunc(fn func(ctx context.Context)) worker {
	return worker{
		fn: fn,
	}
}

func startWorkers(c *cfg) {
	for _, wrk := range c.workers {
		startWorker(c, wrk)
	}
}

func startWorker(c *cfg, wrk worker) {
	c.wg.Add(1)

	go func(w worker) {
		w.fn(c.ctx)
		c.wg.Done()
	}(wrk)
}

func delWorker(c *cfg, name string) {
	for i, worker := range c.workers {
		if worker.name == name {
			c.workers = append(c.workers[:i], c.workers[i+1:]...)
			return
		}
	}
}

func getWorker(c *cfg, name string) *worker {
	for _, wrk := range c.workers {
		if wrk.name == name {
			return &wrk
		}
	}
	return nil
}
