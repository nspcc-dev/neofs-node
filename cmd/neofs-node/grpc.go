package main

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func initGRPC(c *cfg) {
	var err error

	c.cfgGRPC.listener, err = net.Listen("tcp", c.viper.GetString(cfgListenAddress))
	fatalOnErr(err)

	c.cfgGRPC.server = grpc.NewServer(
		grpc.MaxSendMsgSize(c.viper.GetInt(cfgMaxMsgSize)),
	)
}

func serveGRPC(c *cfg) {
	c.wg.Add(1)

	go func() {
		defer func() {
			c.wg.Done()
		}()

		// read more about reflect service at
		// https://github.com/grpc/grpc-go/blob/master/Documentation/server-reflection-tutorial.md
		if c.cfgGRPC.enableReflectService {
			reflection.Register(c.cfgGRPC.server)
		}

		if err := c.cfgGRPC.server.Serve(c.cfgGRPC.listener); err != nil {
			fmt.Println("gRPC server error", err)
		}
	}()
}
