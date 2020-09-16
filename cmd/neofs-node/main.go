package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/nspcc-dev/neofs-node/pkg/util/grace"
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

	initAccountingService(c)
	initContainerService(c)
	initSessionService(c)
	initObjectService(c)
}

func bootUp(c *cfg) {
	serveGRPC(c)
	bootstrapNode(c)
}

func wait(c *cfg) {
	<-c.ctx.Done()
}

func shutdown(c *cfg) {
	c.cfgGRPC.server.GracefulStop()
	fmt.Println("gRPC server stopped")

	c.wg.Wait()
}
