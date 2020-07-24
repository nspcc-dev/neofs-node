package node

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	meta2 "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/meta"
	metrics2 "github.com/nspcc-dev/neofs-node/pkg/services/metrics"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	localstoreParams struct {
		dig.In

		Logger    *zap.Logger
		Buckets   Buckets
		Counter   *atomic.Float64
		Collector metrics2.Collector
	}

	metaIterator struct {
		iter localstore.Iterator
	}
)

func newMetaIterator(iter localstore.Iterator) meta2.Iterator {
	return &metaIterator{iter: iter}
}

func (m *metaIterator) Iterate(handler meta2.IterateFunc) error {
	return m.iter.Iterate(nil, func(objMeta *localstore.ObjectMeta) bool {
		return handler == nil || handler(objMeta.Object) != nil
	})
}

func newLocalstore(p localstoreParams) (localstore.Localstore, error) {
	local, err := localstore.New(localstore.Params{
		BlobBucket: p.Buckets[fsBucket],
		MetaBucket: p.Buckets[boltBucket],
		Logger:     p.Logger,
		Collector:  p.Collector,
	})
	if err != nil {
		return nil, err
	}

	iter := newMetaIterator(local)
	p.Collector.SetCounter(local)
	p.Collector.SetIterator(iter)

	return local, nil
}
