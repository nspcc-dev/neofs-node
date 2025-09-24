package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	httputil "github.com/nspcc-dev/neofs-node/pkg/util/http"
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

	appCfg, err := config.New(config.WithConfigFile(*configFile))
	fatalOnErr(err)

	err = validateConfig(appCfg)
	fatalOnErr(err)

	if *dryRunFlag {
		return
	}

	c := initCfg(appCfg)

	preRunAndLog(c, metricName, initMetrics(c))

	preRunAndLog(c, profilerName, initProfiler(c))

	initApp(c)

	err = c.setShardsCapacity()
	fatalOnErr(err)

	c.setHealthStatus(control.HealthStatus_STARTING)

	bootUp(c)

	c.setHealthStatus(control.HealthStatus_READY)

	wait(c)

	c.setHealthStatus(control.HealthStatus_SHUTTING_DOWN)

	shutdown(c)
}

func preRunAndLog(c *cfg, name string, srv *httputil.Server) {
	c.log.Info(fmt.Sprintf("initializing %s service...", name))
	if srv == nil {
		return
	}

	ln, err := srv.Listen()
	if err != nil {
		c.log.Fatal(fmt.Sprintf("could not init %s service", name),
			zap.Error(err),
		)
		return
	}

	c.log.Info(fmt.Sprintf("%s service is initialized", name))
	c.wg.Add(1)
	go func() {
		runAndLog(c, name, false, func(c *cfg) {
			fatalOnErr(srv.Serve(ln))
			c.wg.Done()
		})
	}()

	c.veryLastClosers[name] = func() {
		c.log.Debug(fmt.Sprintf("shutting down %s service", name))

		err := srv.Shutdown()
		if err != nil {
			c.log.Debug(fmt.Sprintf("could not shutdown  %s server", name),
				zap.Error(err),
			)
		}

		c.log.Debug(fmt.Sprintf("%s service has been stopped", name))
	}
}

func initAndLog(c *cfg, name string, initializer func(*cfg)) {
	c.log.Info(fmt.Sprintf("initializing %s service...", name))
	initializer(c)
	c.log.Info(fmt.Sprintf("%s service has been successfully initialized", name))
}

func initApp(c *cfg) {
	initAndLog(c, "control", initControlService)
	initLocalStorage(c)
	initAndLog(c, "gRPC", initGRPC)

	initAndLog(c, "container", initContainerService)
	initAndLog(c, "storage engine", func(c *cfg) {
		fatalOnErr(c.cfgObject.cfgLocalStorage.localStorage.Open())
		fatalOnErr(c.cfgObject.cfgLocalStorage.localStorage.Init())
	})

	initAndLog(c, "netmap", initNetmapService)
	initAndLog(c, "accounting", initAccountingService)
	initAndLog(c, "session", initSessionService)
	initAndLog(c, "reputation", initReputationService)
	initAndLog(c, "meta", initMeta)
	initAndLog(c, "object", initObjectService)

	initAndLog(c, "morph notifications", listenMorphNotifications)

	c.workers = append(c.workers, newWorkerFromFunc(c.configWatcher))

	c.control.MarkReady(
		c.cfgObject.cfgLocalStorage.localStorage,
		c.netMapSource,
		c.cnrSrc,
		c.replicator,
		c,
	)
}

func runAndLog(c *cfg, name string, logSuccess bool, starter func(*cfg)) {
	c.log.Info(fmt.Sprintf("starting %s service...", name))
	starter(c)

	if logSuccess {
		c.log.Info(fmt.Sprintf("%s service started successfully", name))
	}
}

func bootUp(c *cfg) {
	runAndLog(c, "gRPC", false, serveGRPC)
	runAndLog(c, "notary", true, initNotary)

	bootstrapNode(c)
	startWorkers(c)
}

func wait(c *cfg) {
	c.log.Info("application started",
		zap.String("version", misc.Version))

	select {
	case <-c.ctx.Done(): // graceful shutdown
		c.log.Info("application is shutting down...", zap.String("cause", context.Cause(c.ctx).Error()))
	case err := <-c.internalErr: // internal application error
		c.ctxCancel()

		c.log.Warn("internal application error",
			zap.String("message", err.Error()))
	}
}

func shutdown(c *cfg) {
	for _, closer := range c.closers {
		closer()
	}

	c.veryLastClosersLock.RLock()
	for _, lastCloser := range c.veryLastClosers {
		lastCloser()
	}
	c.veryLastClosersLock.RUnlock()

	c.log.Debug("waiting for all processes to stop")

	c.wg.Wait()
}

func (c *cfg) onShutdown(f func()) {
	c.closers = append(c.closers, f)
}

func (c *cfg) restartMorph() error {
	c.log.Info("restarting internal services because of RPC connection loss...")

	c.resetCaches()

	epoch, ni, err := getNetworkState(c)
	if err != nil {
		return fmt.Errorf("getting network state: %w", err)
	}

	updateLocalState(c, epoch, ni)

	// drop expired sessions if any has appeared while node was sleeping
	c.privateTokenStore.RemoveOld(epoch)

	// bootstrap node after every reconnection cause the longevity of
	// a connection downstate is unpredictable and bootstrap TX is a
	// way to make a heartbeat so nothing is wrong in making sure the
	// node is online (if it should be)

	if !c.needBootstrap() || c.cfgNetmap.reBoostrapTurnedOff.Load() {
		return nil
	}

	err = c.heartbeat()
	if err != nil {
		c.log.Warn("failed to send heartbeat", zap.Error(err))
	}

	c.log.Info("internal services have been restarted after RPC connection loss")

	return nil
}
