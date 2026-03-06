package protobuf

import (
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/mem"
)

// TODO: docs.
type MemBuffer struct {
	mem.SliceBuffer
	ln   int
	refs atomic.Int32
	pool *sync.Pool
}

func (x *MemBuffer) Finalize(ln int) {
	x.ln = ln
	x.Ref()
}

func (x *MemBuffer) Len() int {
	return x.ln
}

func (x *MemBuffer) Ref() {
	if x.refs.Add(1) <= 1 {
		panic("ref of freed buffer")
	}
}

func (x *MemBuffer) Free() {
	switch refs := x.refs.Add(-1); {
	case refs > 0:
	case refs == 0:
		x.pool.Put(x)
	default:
		panic("free of freed buffer")
	}
}

type MemBufferPool struct {
	syncPool *sync.Pool
}

func NewBufferPool(ln int) *MemBufferPool {
	return &MemBufferPool{
		syncPool: &sync.Pool{
			New: func() any {
				return &MemBuffer{
					SliceBuffer: make([]byte, ln),
				}
			},
		},
	}
}

func (x *MemBufferPool) Get() *MemBuffer {
	item := x.syncPool.Get().(*MemBuffer)
	item.pool = x.syncPool
	item.refs.Store(1)
	return item
}
