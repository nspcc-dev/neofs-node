package node

import (
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/meta"
	"github.com/nspcc-dev/neofs-node/lib/metrics"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	localstoreParams struct {
		dig.In

		Logger    *zap.Logger
		Storage   core.Storage
		Counter   *atomic.Float64
		Collector metrics.Collector
	}

	metaIterator struct {
		iter localstore.Iterator
	}
)

func newMetaIterator(iter localstore.Iterator) meta.Iterator {
	return &metaIterator{iter: iter}
}

func (m *metaIterator) Iterate(handler meta.IterateFunc) error {
	return m.iter.Iterate(nil, func(objMeta *localstore.ObjectMeta) bool {
		return handler == nil || handler(objMeta.Object) != nil
	})
}

func newLocalstore(p localstoreParams) (localstore.Localstore, error) {
	metaBucket, err := p.Storage.GetBucket(core.MetaStore)
	if err != nil {
		return nil, err
	}

	blobBucket, err := p.Storage.GetBucket(core.BlobStore)
	if err != nil {
		return nil, err
	}

	local, err := localstore.New(localstore.Params{
		BlobBucket: blobBucket,
		MetaBucket: metaBucket,
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
