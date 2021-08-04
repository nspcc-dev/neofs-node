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
	grpcconfig.IterateEndpoints(c.appCfg, func(sc *grpcconfig.Config) {
		lis, err := net.Listen("tcp", sc.Endpoint())
		fatalOnErr(err)

		c.cfgGRPC.listeners = append(c.cfgGRPC.listeners, lis)

		serverOpts := []grpc.ServerOption{
			grpc.MaxSendMsgSize(maxMsgSize),
		}

		tlsCfg := sc.TLS()

		if tlsCfg != nil {
			creds, err := credentials.NewServerTLSFromFile(tlsCfg.CertificateFile(), tlsCfg.KeyFile())
			fatalOnErrDetails("could not read credentials from file", err)

			serverOpts = append(serverOpts, grpc.Creds(creds))
		}

		srv := grpc.NewServer(serverOpts...)

		c.onShutdown(func() {
			stopGRPC("NeoFS Public API", srv, c.log)
		})

		c.cfgGRPC.servers = append(c.cfgGRPC.servers, srv)
	})
}

func serveGRPC(c *cfg) {
	for i := range c.cfgGRPC.servers {
		c.wg.Add(1)

		srv := c.cfgGRPC.servers[i]
		lis := c.cfgGRPC.listeners[i]

		go func() {
			defer func() {
				c.log.Info("stop listening gRPC endpoint",
					zap.String("endpoint", lis.Addr().String()),
				)

				c.wg.Done()
			}()

			c.log.Info("start listening gRPC endpoint",
				zap.String("endpoint", lis.Addr().String()),
			)

			if err := srv.Serve(lis); err != nil {
				fmt.Println("gRPC server error", err)
			}
		}()
	}
}

func stopGRPC(name string, s *grpc.Server, l *logger.Logger) {
	l = l.With(zap.String("name", name))

	l.Info("stopping gRPC server...")

	s.GracefulStop()

	l.Info("gRPC server stopped successfully")
}
