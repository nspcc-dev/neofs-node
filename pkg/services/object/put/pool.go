package putsvc

import (
	"sync"
)

const defaultAllocSize = 1024

var putBytesPool = &sync.Pool{
	New: func() any { return make([]byte, 0, defaultAllocSize) },
}

func getPayload() []byte {
	return putBytesPool.Get().([]byte)
}

func putPayload(p []byte) {
	//nolint:staticcheck
	putBytesPool.Put(p[:0])
}
