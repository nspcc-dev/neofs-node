package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"go.uber.org/zap"
)

const (
	// SuccessReturnCode returns when application closed without panic.
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
	dryRunFlag := flag.Bool("check", false, "validate configuration and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Print(misc.BuildInfo("NeoFS Storage node"))

		os.Exit(SuccessReturnCode)
	}

	appCfg := config.New(config.Prm{}, config.WithConfigFile(*configFile))

	err := validateConfig(appCfg)
	fatalOnErr(err)

	if *dryRunFlag {
		return
	}

	c := initCfg(appCfg)

	initApp(c)

	c.setHealthStatus(control.HealthStatus_STARTING)

	bootUp(c)

	c.setHealthStatus(control.HealthStatus_READY)

	wait(c)

	c.setHealthStatus(control.HealthStatus_SHUTTING_DOWN)

	shutdown(c)
}

func initAndLog(c *cfg, name string, initializer func(*cfg)) {
	c.log.Info(fmt.Sprintf("initializing %s service...", name))
	initializer(c)
	c.log.Info(fmt.Sprintf("%s service has been successfully initialized", name))
}

func initApp(c *cfg) {
	initLocalStorage(c)

	c.ctx, c.ctxCancel = signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	initAndLog(c, "storage engine", func(c *cfg) {
		fatalOnErr(c.cfgObject.cfgLocalStorage.localStorage.Open())
		fatalOnErr(c.cfgObject.cfgLocalStorage.localStorage.Init())
	})

	initAndLog(c, "gRPC", initGRPC)
	initAndLog(c, "netmap", initNetmapService)
	initAndLog(c, "accounting", initAccountingService)
	initAndLog(c, "container", initContainerService)
	initAndLog(c, "session", initSessionService)
	initAndLog(c, "reputation", initReputationService)
	initAndLog(c, "notification", initNotifications)
	initAndLog(c, "object", initObjectService)
	initAndLog(c, "pprof", initProfiler)
	initAndLog(c, "prometheus", initMetrics)
	initAndLog(c, "tree", initTreeService)
	initAndLog(c, "control", initControlService)

	initAndLog(c, "morph notifications", listenMorphNotifications)

	c.workers = append(c.workers, newWorkerFromFunc(c.configWatcher))
}

func runAndLog(c *cfg, name string, logSuccess bool, starter func(*cfg)) {
	c.log.Info(fmt.Sprintf("starting %s service...", name))
	starter(c)

	if logSuccess {
		c.log.Info(fmt.Sprintf("%s service started successfully", name))
	}
}

func bootUp(c *cfg) {
	runAndLog(c, "NATS", true, connectNats)
	runAndLog(c, "gRPC", false, serveGRPC)
	runAndLog(c, "notary", true, makeAndWaitNotaryDeposit)

	bootstrapNode(c)
	startWorkers(c)
}

func wait(c *cfg) {
	c.log.Info("application started",
		zap.String("version", misc.Version))

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
