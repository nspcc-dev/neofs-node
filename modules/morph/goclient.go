package morph

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/goclient"
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

func newMorphClient(p morphClientParams) (*goclient.Client, error) {
	return goclient.New(context.Background(), &goclient.Params{
		Log:         p.Logger,
		Key:         p.Key,
		Endpoint:    p.Viper.GetString(optPath(prefix, endpointOpt)),
		DialTimeout: p.Viper.GetDuration(optPath(prefix, dialTimeoutOpt)),
		Magic:       netmode.Magic(p.Viper.GetUint32(optPath(prefix, magicNumberOpt))),
	})
}
