package putsvc

import (
	"sync"
)

var buffers sync.Pool

func getBuffer(cp int) []byte {
	b, ok := buffers.Get().([]byte)
	if ok {
		if cap(b) >= cp {
			return b
		}
		buffers.Put(b)
	}
	return make([]byte, 0, cp)
}

func putBuffer(p []byte) {
	//nolint:staticcheck
	buffers.Put(p[:0])
}
