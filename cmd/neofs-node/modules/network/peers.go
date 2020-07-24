package network

import (
	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/pkg/network/peers"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type peersParams struct {
	dig.In

	Viper   *viper.Viper
	Logger  *zap.Logger
	Address multiaddr.Multiaddr
}

func newPeers(p peersParams) (peers.Interface, error) {
	return peers.New(peers.Params{
		Logger:           p.Logger,
		ConnectionTTL:    p.Viper.GetDuration("peers.connections_ttl"),
		ConnectionIDLE:   p.Viper.GetDuration("peers.connections_idle"),
		MetricsTimeout:   p.Viper.GetDuration("peers.metrics_timeout"),
		KeepAliveTTL:     p.Viper.GetDuration("peers.keep_alive.ttl"),
		KeepAlivePingTTL: p.Viper.GetDuration("peers.keep_alive.ping"),
	})
}
