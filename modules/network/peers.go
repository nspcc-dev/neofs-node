package network

import (
	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type peersParams struct {
	dig.In

	Viper     *viper.Viper
	Logger    *zap.Logger
	Address   multiaddr.Multiaddr
	Transport transport.Transport
}

func newTransport(v *viper.Viper) transport.Transport {
	return transport.New(
		v.GetInt64("transport.attempts_count"),
		v.GetDuration("transport.attempts_ttl"),
	)
}

func newPeers(p peersParams) (peers.Interface, error) {
	return peers.New(peers.Params{
		Logger:           p.Logger,
		Address:          p.Address,
		Transport:        p.Transport,
		Attempts:         p.Viper.GetInt64("peers.attempts_count"),
		AttemptsTTL:      p.Viper.GetDuration("peers.attempts_ttl"),
		ConnectionTTL:    p.Viper.GetDuration("peers.connections_ttl"),
		ConnectionIDLE:   p.Viper.GetDuration("peers.connections_idle"),
		MetricsTimeout:   p.Viper.GetDuration("peers.metrics_timeout"),
		KeepAliveTTL:     p.Viper.GetDuration("peers.keep_alive.ttl"),
		KeepAlivePingTTL: p.Viper.GetDuration("peers.keep_alive.ping"),
	})
}
