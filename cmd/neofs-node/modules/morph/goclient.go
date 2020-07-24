package morph

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type morphClientParams struct {
	dig.In

	Viper *viper.Viper

	Logger *zap.Logger

	Key *ecdsa.PrivateKey
}

func newClient(p morphClientParams) (*client.Client, error) {
	return client.New(
		p.Key,
		p.Viper.GetString(optPath(prefix, endpointOpt)),
		client.WithLogger(p.Logger),
		client.WithDialTimeout(p.Viper.GetDuration(optPath(prefix, dialTimeoutOpt))),
		client.WithMagic(netmode.Magic(p.Viper.GetUint32(optPath(prefix, magicNumberOpt)))),
	)
}
