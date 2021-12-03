package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"go.uber.org/zap"
)

const (
	// SuccessReturnCode returns when application closed without panic
	SuccessReturnCode = 0
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
	versionFlag := flag.Bool("version", false, "neofs node version")
	flag.Parse()

	if *versionFlag {
		fmt.Printf(
			"Version: %s \nBuild: %s \nDebug: %s\n",
			misc.Version,
			misc.Build,
			misc.Debug,
		)

		os.Exit(SuccessReturnCode)
	}

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
	makeAndWaitNotaryDeposit(c)
	bootstrapNode(c)
	startWorkers(c)
	startBlockTimers(c)
}

func wait(c *cfg) {
	c.log.Info("application started",
		zap.String("build_time", misc.Build),
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

	c.log.Debug("waiting for all processes to stop")

	c.wg.Wait()
}

func (c *cfg) onShutdown(f func()) {
	c.closers = append(c.closers, f)
}
