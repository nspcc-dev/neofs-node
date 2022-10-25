package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func initGRPC(c *cfg) {
	var successCount int
	grpcconfig.IterateEndpoints(c.appCfg, func(sc *grpcconfig.Config) {
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

			var cipherSuites []uint16
			if !tlsCfg.UseInsecureCrypto() {
				// This more or less follows the list in https://wiki.mozilla.org/Security/Server_Side_TLS
				// excluding:
				// 1. TLS 1.3 suites need not be specified here.
				// 2. Suites that use DH key exchange are not implemented by stdlib.
				cipherSuites = []uint16{
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
					tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
				}
			}
			creds := credentials.NewTLS(&tls.Config{
				MinVersion:   tls.VersionTLS12,
				CipherSuites: cipherSuites,
				Certificates: []tls.Certificate{cert},
			})

			serverOpts = append(serverOpts, grpc.Creds(creds))
		}

		lis, err := net.Listen("tcp", sc.Endpoint())
		if err != nil {
			c.log.Error("can't listen gRPC endpoint", zap.Error(err))
			return
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

func stopGRPC(name string, s *grpc.Server, l *logger.Logger) {
	l = &logger.Logger{Logger: l.With(zap.String("name", name))}

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
