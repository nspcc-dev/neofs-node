package main

import (
	"log"

	"github.com/nspcc-dev/neofs-node/pkg/util/grace"
)

func fatalOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	c := defaultCfg()

	c.ctx = grace.NewGracefulContext(nil)

	serveGRPC(c)

	<-c.ctx.Done()

	c.wg.Wait()
}
