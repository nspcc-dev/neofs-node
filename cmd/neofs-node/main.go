package main

import (
	"fmt"
	"log"

	object "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	objectGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/util/grace"
)

func fatalOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	c := defaultCfg()

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

	object.RegisterObjectServiceServer(c.cfgGRPC.server, objectGRPC.New(new(objectSvc)))
}

func bootUp(c *cfg) {
	serveGRPC(c)
}

func wait(c *cfg) {
	<-c.ctx.Done()
}

func shutdown(c *cfg) {
	c.cfgGRPC.server.GracefulStop()
	fmt.Println("gRPC server stopped")

	c.wg.Wait()
}
