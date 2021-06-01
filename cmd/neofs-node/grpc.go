package main

import (
	"fmt"
	"net"

	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func initGRPC(c *cfg) {
	var err error

	c.cfgGRPC.listener, err = net.Listen("tcp", grpcconfig.Endpoint(c.appCfg))
	fatalOnErr(err)

	serverOpts := []grpc.ServerOption{
		grpc.MaxSendMsgSize(maxMsgSize),
	}

	if c.cfgGRPC.tlsEnabled {
		creds, err := credentials.NewServerTLSFromFile(c.cfgGRPC.tlsCertFile, c.cfgGRPC.tlsKeyFile)
		fatalOnErrDetails("could not read credentials from file", err)

		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	c.cfgGRPC.server = grpc.NewServer(serverOpts...)

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
