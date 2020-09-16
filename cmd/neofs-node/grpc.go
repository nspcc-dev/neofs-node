package main

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
)

func initGRPC(c *cfg) {
	var err error

	c.cfgGRPC.listener, err = net.Listen("tcp", c.viper.GetString(cfgListenAddress))
	fatalOnErr(err)

	c.cfgGRPC.server = grpc.NewServer()
}

func serveGRPC(c *cfg) {
	go func() {
		c.wg.Add(1)
		defer func() {
			c.wg.Done()
		}()

		if err := c.cfgGRPC.server.Serve(c.cfgGRPC.listener); err != nil {
			fmt.Println("gRPC server error", err)
		}
	}()
}
