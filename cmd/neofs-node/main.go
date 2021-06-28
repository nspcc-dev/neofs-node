package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"go.uber.org/zap"
)

// prints err to standard logger and calls os.Exit(1).
func fatalOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// prints err with details to standard logger and calls os.Exit(1).
func fatalOnErrDetails(details string, err error) {
	if err != nil {
		log.Fatal(fmt.Errorf("%s: %w", details, err))
	}
}

func main() {
	configFile := flag.String("config", "", "path to config")
	flag.Parse()

	c := initCfg(*configFile)

	initApp(c)

	c.setHealthStatus(control.HealthStatus_STARTING)

	bootUp(c)

	c.setHealthStatus(control.HealthStatus_READY)

	wait(c)

	c.setHealthStatus(control.HealthStatus_SHUTTING_DOWN)

	shutdown(c)
}

func initApp(c *cfg) {
	c.ctx, c.ctxCancel = signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	initGRPC(c)

	initNetmapService(c)
	initAccountingService(c)
	initContainerService(c)
	initSessionService(c)
	initReputationService(c)
	initObjectService(c)
	initProfiler(c)
	initMetrics(c)
	initControlService(c)

	fatalOnErr(c.cfgObject.cfgLocalStorage.localStorage.Open())
	fatalOnErr(c.cfgObject.cfgLocalStorage.localStorage.Init())

	listenMorphNotifications(c)
}

func bootUp(c *cfg) {
	serveGRPC(c)
	bootstrapNode(c)
	startWorkers(c)
	startBlockTimers(c)
}

func wait(c *cfg) {
	c.log.Info("application started",
		zap.String("build time", misc.Build),
		zap.String("version", misc.Version),
		zap.String("debug", misc.Debug),
	)

	select {
	case <-c.ctx.Done(): // graceful shutdown
	case err := <-c.internalErr: // internal application error
		close(c.internalErr)
		c.ctxCancel()

		c.log.Warn("internal application error",
			zap.String("message", err.Error()))
	}
}

func shutdown(c *cfg) {
	for _, closer := range c.closers {
		closer()
	}

	c.wg.Wait()
}

func (c *cfg) onShutdown(f func()) {
	c.closers = append(c.closers, f)
}
