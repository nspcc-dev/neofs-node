package v2

import (
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
)

type xHeaderSource interface {
	GetXHeaders() []eaclSDK.Header
}

type requestXHeaderSource struct {
	req Request
}

type responseXHeaderSource struct {
	resp Response

	req Request
}

func (s requestXHeaderSource) GetXHeaders() []eaclSDK.Header {
	ln := 0

	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		ln += len(meta.GetXHeaders())
	}

	res := make([]eaclSDK.Header, 0, ln)
	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		x := meta.GetXHeaders()
		for i := range x {
			res = append(res, xHeader{x[i].GetKey(), x[i].GetValue()})
		}
	}

	return res
}

func (s responseXHeaderSource) GetXHeaders() []eaclSDK.Header {
	ln := 0
	xHdrs := make([][]*protosession.XHeader, 0)

	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		x := meta.GetXHeaders()

		ln += len(x)

		xHdrs = append(xHdrs, x)
	}

	res := make([]eaclSDK.Header, 0, ln)

	for i := range xHdrs {
		for j := range xHdrs[i] {
			res = append(res, xHeader{xHdrs[i][j].GetKey(), xHdrs[i][j].GetValue()})
		}
	}

	return res
}
