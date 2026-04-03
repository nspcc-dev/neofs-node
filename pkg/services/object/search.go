package object

import (
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-sdk-go/client"
)

const maxSearchResultLen = 1000

type searchResultBufferPoolImpl struct {
	syncPool sync.Pool
}

func (x *searchResultBufferPoolImpl) Get(ln uint16) []client.SearchResultItem {
	if ln > maxSearchResultLen {
		panic(fmt.Sprintf("too big buffer requested from pool: %d > %d", ln, maxSearchResultLen))
	}
	return x.syncPool.Get().([]client.SearchResultItem)[:ln]
}

func (x *searchResultBufferPoolImpl) Put(buf []client.SearchResultItem) {
	if cap(buf) != maxSearchResultLen {
		panic(fmt.Sprintf("attempt to return buffer with wrong capacity into pool: %d instead of %d", cap(buf), maxSearchResultLen))
	}
	x.syncPool.Put(buf)
}

var seachResultBufferPool = &searchResultBufferPoolImpl{
	syncPool: sync.Pool{
		New: func() any {
			return make([]client.SearchResultItem, maxSearchResultLen)
		},
	},
}
