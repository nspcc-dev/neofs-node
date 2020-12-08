package getsvc

type RangeHashRes struct {
	hashes [][]byte
}

func (r *RangeHashRes) Hashes() [][]byte {
	return r.hashes
}
