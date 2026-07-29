package v2

import (
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
)

type xHeaderSource interface {
	GetXHeaders() []eacl.Header
}

type requestXHeaderSource struct {
	req Request
}

type responseXHeaderSource struct {
	resp Response

	req requestXHeaderSource
}

func (s requestXHeaderSource) GetXHeaders() []eacl.Header {
	x := s.req.GetMetaHeader().GetXHeaders()
	res := make([]eacl.Header, 0, len(x))
	for i := range x {
		res = append(res, xHeader{x[i].GetKey(), x[i].GetValue()})
	}

	return res
}

func (s responseXHeaderSource) GetXHeaders() []eacl.Header {
	return s.req.GetXHeaders()
}
