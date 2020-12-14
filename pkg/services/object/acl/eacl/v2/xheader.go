package v2

import (
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type xHeaderSource interface {
	GetXHeaders() []*session.XHeader
}

type requestXHeaderSource struct {
	req Request
}

type responseXHeaderSource struct {
	resp Response

	addr *refs.Address
}

func (s *requestXHeaderSource) GetXHeaders() []*session.XHeader {
	ln := 0
	xHdrs := make([][]*session.XHeader, 0)

	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		x := meta.GetXHeaders()

		ln += len(x)

		xHdrs = append(xHdrs, x)
	}

	res := make([]*session.XHeader, 0, ln)

	for i := range xHdrs {
		for j := range xHdrs[i] {
			res = append(res, xHdrs[i][j])
		}
	}

	return res
}

func (s *responseXHeaderSource) GetXHeaders() []*session.XHeader {
	ln := 0
	xHdrs := make([][]*session.XHeader, 0)

	for meta := s.resp.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		x := meta.GetXHeaders()

		ln += len(x)

		xHdrs = append(xHdrs, x)
	}

	res := make([]*session.XHeader, 0, ln)

	for i := range xHdrs {
		for j := range xHdrs[i] {
			res = append(res, xHdrs[i][j])
		}
	}

	return res
}
