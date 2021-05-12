package main

import (
	"fmt"
	"net"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func initGRPC(c *cfg) {
	var err error

	c.cfgGRPC.listener, err = net.Listen("tcp", c.viper.GetString(cfgListenAddress))
	fatalOnErr(err)

	c.cfgGRPC.server = grpc.NewServer(
		grpc.MaxSendMsgSize(maxMsgSize),
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
