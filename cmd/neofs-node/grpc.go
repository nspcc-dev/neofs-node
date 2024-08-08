package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"time"

	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func initGRPC(c *cfg) {
	if c.cfgMorph.client == nil {
		initMorphComponents(c)
	}

	// limit max size of single messages received by the gRPC servers up to max
	// object size setting of the NeoFS network: this is needed to serve
	// ObjectService.Replicate RPC transmitting the entire stored object in one
	// message
	maxObjSize, err := c.nCli.MaxObjectSize()
	fatalOnErrDetails("read max object size network setting to determine gRPC recv message limit", err)

	maxRecvSize := maxObjSize
	// don't forget about meta fields: object header + other ObjectService.Replicate
	// request fields. For the latter, less is needed now, but it is still better to
	// take with a reserve for potential protocol extensions. Anyway, 1 KB is
	// nothing IRL.
	const maxMetadataSize = object.MaxHeaderLen + 1<<10
	if maxRecvSize < uint64(math.MaxUint64-maxMetadataSize) { // just in case, always true in practice
		maxRecvSize += maxMetadataSize
	} else {
		maxRecvSize = math.MaxUint64
	}

	var maxRecvMsgSizeOpt grpc.ServerOption
	if maxRecvSize > maxMsgSize { // do not decrease default value
		if maxRecvSize > math.MaxInt {
			// ^2GB for 32-bit systems which is currently enough in practice. If at some
			// point this is not enough, we'll need to expand the option
			fatalOnErr(fmt.Errorf("cannot serve NeoFS API over gRPC: object of max size is bigger than gRPC server is able to support %d>%d",
				maxRecvSize, math.MaxInt))
		}
		maxRecvMsgSizeOpt = grpc.MaxRecvMsgSize(int(maxRecvSize))
		c.log.Debug("limit max recv gRPC message size to fit max stored objects",
			zap.Uint64("max object size", maxObjSize), zap.Uint64("max recv msg", maxRecvSize))
	}

	var successCount int
	grpcconfig.IterateEndpoints(c.cfgReader, func(sc *grpcconfig.Config) {
		serverOpts := []grpc.ServerOption{
			grpc.MaxSendMsgSize(maxMsgSize),
		}
		if maxRecvMsgSizeOpt != nil {
			// TODO(@cthulhu-rider): the setting can be server-global only now, support
			//  per-RPC limits
			// TODO(@cthulhu-rider): max object size setting may change in general,
			//  but server configuration is static now
			serverOpts = append(serverOpts, maxRecvMsgSizeOpt)
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
