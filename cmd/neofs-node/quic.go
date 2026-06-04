package main

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/internal/qstream"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"github.com/quic-go/quic-go"
	"go.uber.org/zap"
)

func initQUIC(c *cfg, server *objectService.Server) {
	for _, sc := range c.appCfg.GRPC {
		endpoint := sc.Endpoint

		tlsCfg, err := qstream.ServerTLSConfig()
		if err != nil {
			c.log.Error("QUIC: failed to build TLS config", zap.String("endpoint", endpoint), zap.Error(err))
			continue
		}

		ln, err := quic.ListenAddr(endpoint, tlsCfg, qstream.Config())
		if err != nil {
			c.log.Error("QUIC: failed to listen", zap.String("endpoint", endpoint), zap.Error(err))
			continue
		}

		c.cfgGRPC.quicListeners = append(c.cfgGRPC.quicListeners, ln)
		c.log.Info("QUIC: start listening GET-stream endpoint", zap.String("endpoint", endpoint))

		c.wg.Go(func() {
			serveQUICListener(c, ln, server)
		})
	}

	c.onShutdown(func() {
		for _, ln := range c.cfgGRPC.quicListeners {
			_ = ln.Close()
		}
	})
}

func serveQUICListener(c *cfg, ln *quic.Listener, server *objectService.Server) {
	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			if !errors.Is(err, quic.ErrServerClosed) {
				c.log.Error("QUIC: accept connection failed", zap.Stringer("endpoint", ln.Addr()), zap.Error(err))
			}
			return
		}
		go serveQUICConn(conn, server)
	}
}

func serveQUICConn(conn *quic.Conn, server *objectService.Server) {
	for {
		st, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		go server.ServeGetStream(st.Context(), st)
	}
}
