package localstore

import (
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/lib/metrics"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (l *localstore) Del(key refs.Address) error {
	k, err := key.Hash()
	if err != nil {
		return errors.Wrap(err, "Localstore Del failed on key.Marshal")
	}

	// try to fetch object for metrics
	obj, err := l.Get(key)
	if err != nil {
		l.log.Warn("localstore Del failed on localstore.Get", zap.Error(err))
	}

	if err := l.blobBucket.Del(k); err != nil {
		l.log.Warn("Localstore Del failed on BlobBucket.Del", zap.Error(err))
	}

	if err := l.metaBucket.Del(k); err != nil {
		return errors.Wrap(err, "Localstore Del failed on MetaBucket.Del")
	}

	if obj != nil {
		l.col.UpdateContainer(
			key.CID,
			obj.SystemHeader.PayloadLength,
			metrics.RemSpace)
	}

	return nil
}
