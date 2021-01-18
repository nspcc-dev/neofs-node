package main

import (
	"fmt"
	"net"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
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

	c.onShutdown(func() {
		stopGRPC("NeoFS Public API", c.cfgGRPC.server, c.log)
	})
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

func stopGRPC(name string, s *grpc.Server, l *logger.Logger) {
	l = l.With(zap.String("name", name))

	l.Info("stopping gRPC server...")

	s.GracefulStop()

	l.Info("gRPC server stopped successfully")
}
