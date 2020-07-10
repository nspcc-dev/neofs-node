package metrics

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/lib/meta"
)

type metaWrapper struct {
	sync.Mutex
	iter meta.Iterator
}

func newMetaWrapper() *metaWrapper {
	return &metaWrapper{}
}

func (m *metaWrapper) changeIter(iter meta.Iterator) {
	m.Lock()
	m.iter = iter
	m.Unlock()
}

func (m *metaWrapper) Iterate(h meta.IterateFunc) error {
	m.Lock()
	defer m.Unlock()

	if m.iter == nil {
		return errEmptyMetaStore
	}

	return m.iter.Iterate(h)
}
