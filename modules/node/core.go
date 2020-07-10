package node

import (
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/storage"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func listBuckets(v *viper.Viper) []core.BucketType {
	var (
		items  = v.GetStringMap("storage")
		result = make([]core.BucketType, 0, len(items))
	)

	for name := range items {
		result = append(result, core.BucketType(name))
	}

	return result
}

func newStorage(l *zap.Logger, v *viper.Viper) (core.Storage, error) {
	return storage.New(storage.Params{
		Viper:   v,
		Logger:  l,
		Buckets: listBuckets(v),
	})
}
