package network

import (
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/pkg/network/muxer"
	"github.com/spf13/viper"
	"github.com/valyala/fasthttp"
	"go.uber.org/dig"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type muxerParams struct {
	dig.In

	Logger *zap.Logger
	P2P    *grpc.Server

	Address     multiaddr.Multiaddr
	ShutdownTTL time.Duration `name:"shutdown_ttl"`
	API         fasthttp.RequestHandler
	Viper       *viper.Viper
}

const appName = "neofs-node"

func newFastHTTPServer(p muxerParams) *fasthttp.Server {
	srv := new(fasthttp.Server)
	srv.Name = appName
	srv.ReadBufferSize = p.Viper.GetInt("muxer.http.read_buffer_size")
	srv.WriteBufferSize = p.Viper.GetInt("muxer.http.write_buffer_size")
	srv.ReadTimeout = p.Viper.GetDuration("muxer.http.read_timeout")
	srv.WriteTimeout = p.Viper.GetDuration("muxer.http.write_timeout")
	srv.GetOnly = true
	srv.DisableHeaderNamesNormalizing = true
	srv.NoDefaultServerHeader = true
	srv.NoDefaultContentType = true
	srv.Handler = p.API

	return srv
}

func newMuxer(p muxerParams) muxer.Mux {
	return muxer.New(muxer.Params{
		P2P:         p.P2P,
		Logger:      p.Logger,
		Address:     p.Address,
		ShutdownTTL: p.ShutdownTTL,
		API:         newFastHTTPServer(p),
	})
}
