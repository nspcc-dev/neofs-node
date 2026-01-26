package getsvc

import "sync"

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, streamChunkSize)
		return &b
	},
}
