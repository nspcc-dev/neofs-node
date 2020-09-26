package rangesvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

type Result struct {
	head *object.Object

	stream Streamer
}

type Response struct {
	chunk []byte
}

func (r *Response) PayloadChunk() []byte {
	return r.chunk
}

func (r *Result) Head() *object.Object {
	return r.head
}

func (r *Result) Stream() Streamer {
	return r.stream
}
