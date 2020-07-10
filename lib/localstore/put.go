package localstore

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/lib/metrics"
	"github.com/pkg/errors"
)

func (l *localstore) Put(ctx context.Context, obj *Object) error {
	var (
		oa   refs.Address
		k, v []byte
		err  error
	)

	oa = *obj.Address()
	k, err = oa.Hash()

	if err != nil {
		return errors.Wrap(err, "Localstore Put failed on StorageKey.marshal")
	}

	if v, err = obj.Marshal(); err != nil {
		return errors.Wrap(err, "Localstore Put failed on blobValue")
	}

	if err = l.blobBucket.Set(k, v); err != nil {
		return errors.Wrap(err, "Localstore Put failed on BlobBucket.Set")
	}

	if v, err = metaFromObject(ctx, obj).Marshal(); err != nil {
		return errors.Wrap(err, "Localstore Put failed on metaValue")
	}

	if err = l.metaBucket.Set(k, v); err != nil {
		return errors.Wrap(err, "Localstore Put failed on MetaBucket.Set")
	}

	l.col.UpdateContainer(
		obj.SystemHeader.CID,
		obj.SystemHeader.PayloadLength,
		metrics.AddSpace)

	return nil
}
