package main

import (
	"flag"
	"log"

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

	init_(c)

	bootUp(c)

	wait(c)

	shutdown(c)
}

func init_(c *cfg) {
	c.ctx = grace.NewGracefulContext(nil)

	initGRPC(c)

	initNetmapService(c)
	initAccountingService(c)
	initContainerService(c)
	initSessionService(c)
	initObjectService(c)
	initProfiler(c)

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

	<-c.ctx.Done()
}

func shutdown(c *cfg) {
	if err := c.cfgObject.metastorage.Close(); err != nil {
		c.log.Error("could not close metabase",
			zap.String("error", err.Error()),
		)
	}

	c.cfgGRPC.server.GracefulStop()

	c.log.Info("gRPC server stopped")

	c.wg.Wait()
}
