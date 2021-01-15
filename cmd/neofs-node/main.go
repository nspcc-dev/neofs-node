package main

import (
	"context"
	"flag"
	"log"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/nspcc-dev/neofs-node/pkg/util/grace"
	"go.uber.org/zap"
)

func fatalOnErr(err error) {
	if err != nil {
		log.Fatal(err)
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
	c.ctx, c.ctxCancel = context.WithCancel(grace.NewGracefulContext(nil))

	initGRPC(c)

	initNetmapService(c)
	initAccountingService(c)
	initContainerService(c)
	initSessionService(c)
	initObjectService(c)
	initProfiler(c)
	initControlService(c)

	fatalOnErr(c.cfgObject.cfgLocalStorage.localStorage.Open())
	fatalOnErr(c.cfgObject.cfgLocalStorage.localStorage.Init())

	listenMorphNotifications(c)
}

func bootUp(c *cfg) {
	serveProfiler(c)
	serveGRPC(c)
	bootstrapNode(c)
	startWorkers(c)
}

func wait(c *cfg) {
	c.log.Info("application started")

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
	c.cfgGRPC.server.GracefulStop()
	c.cfgControlService.server.GracefulStop()

	c.log.Info("gRPC server stopped")

	goOffline(c)

	c.wg.Wait()
}
