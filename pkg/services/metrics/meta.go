package metrics

import (
	"sync"

	meta2 "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/meta"
)

type metaWrapper struct {
	sync.Mutex
	iter meta2.Iterator
}

func newMetaWrapper() *metaWrapper {
	return &metaWrapper{}
}

func (m *metaWrapper) changeIter(iter meta2.Iterator) {
	m.Lock()
	m.iter = iter
	m.Unlock()
}

func (m *metaWrapper) Iterate(h meta2.IterateFunc) error {
	m.Lock()
	defer m.Unlock()

	if m.iter == nil {
		return errEmptyMetaStore
	}

	return m.iter.Iterate(h)
}
