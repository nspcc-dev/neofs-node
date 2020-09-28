package rangehashsvc

type Response struct {
	hashes [][]byte
}

func (r *Response) Hashes() [][]byte {
	return r.hashes
}
