package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
)

type httpComponent struct {
	address     string
	name        string
	handler     http.Handler
	shutdownDur time.Duration
	enabled     bool
	cfg         *cfg
	preReload   func(c *cfg)
}

func (cmp *httpComponent) init(c *cfg) {
	if !cmp.enabled {
		c.log.Info(fmt.Sprintf("%s is disabled", cmp.name))
		return
	}
	// Init server with parameters
	srv := httputil.New(
		*httputil.NewHTTPSrvPrm(
			cmp.address,
			cmp.handler,
		),
		httputil.WithShutdownTimeout(
			cmp.shutdownDur,
		),
	)
	c.closers = append(c.closers, closer{
		cmp.name,
		func() { stopAndLog(c, cmp.name, srv.Shutdown) },
	})
	c.workers = append(c.workers, worker{
		cmp.name,
		func(ctx context.Context) {
			runAndLog(c, cmp.name, false, func(c *cfg) {
				fatalOnErr(srv.Serve())
			})
		},
	})
}

func (cmp *httpComponent) reload() error {
	if cmp.preReload != nil {
		cmp.preReload(cmp.cfg)
	}
	// Shutdown server
	closer := getCloser(cmp.cfg, cmp.name)
	if closer != nil {
		closer.fn()
	}
	// Cleanup
	delCloser(cmp.cfg, cmp.name)
	delWorker(cmp.cfg, cmp.name)
	// Init server with new parameters
	cmp.init(cmp.cfg)
	// Start worker
	if cmp.enabled {
		startWorker(cmp.cfg, *getWorker(cmp.cfg, cmp.name))
	}
	return nil
}
