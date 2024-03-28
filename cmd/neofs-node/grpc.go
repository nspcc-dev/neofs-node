package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"go.uber.org/zap"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func initGRPC(c *cfg) {
	var successCount int
	grpcconfig.IterateEndpoints(c.cfgReader, func(sc *grpcconfig.Config) {
		serverOpts := []grpc.ServerOption{
			grpc.MaxSendMsgSize(maxMsgSize),
		}

		tlsCfg := sc.TLS()

		if tlsCfg != nil {
			cert, err := tls.LoadX509KeyPair(tlsCfg.CertificateFile(), tlsCfg.KeyFile())
			if err != nil {
				c.log.Error("could not read certificate from file", zap.Error(err))
				return
			}

			creds := credentials.NewTLS(&tls.Config{
				Certificates: []tls.Certificate{cert},
			})

			serverOpts = append(serverOpts, grpc.Creds(creds))
		}

		lis, err := net.Listen("tcp", sc.Endpoint())
		if err != nil {
			c.log.Error("can't listen gRPC endpoint", zap.Error(err))
			return
		}

		if connLimit := sc.ConnectionLimit(); connLimit > 0 {
			lis = netutil.LimitListener(lis, connLimit)
		}

		c.cfgGRPC.listeners = append(c.cfgGRPC.listeners, lis)

		srv := grpc.NewServer(serverOpts...)

		c.onShutdown(func() {
			stopGRPC("NeoFS Public API", srv, c.log)
		})

		c.cfgGRPC.servers = append(c.cfgGRPC.servers, srv)
		successCount++
	})

	if successCount == 0 {
		fatalOnErr(errors.New("could not listen to any gRPC endpoints"))
	}
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

func stopGRPC(name string, s *grpc.Server, l *zap.Logger) {
	l = l.With(zap.String("name", name))

	l.Info("stopping gRPC server...")

	// GracefulStop() may freeze forever, see #1270
	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Minute):
		l.Info("gRPC cannot shutdown gracefully, forcing stop")
		s.Stop()
	}

	l.Info("gRPC server stopped successfully")
}
