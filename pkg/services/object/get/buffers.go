package getsvc

import (
	"sync"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
)

const (
	maxGetFirstECPartRequestLen = 512 // a bit bigger than needed
)

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, streamChunkSize)
		return &b
	},
}

var getECPartRequestBufferPool = iprotobuf.NewBufferPool(maxGetFirstECPartRequestLen)
