package rangesvc

type Response struct {
	chunk []byte
}

func (r *Response) PayloadChunk() []byte {
	return r.chunk
}
