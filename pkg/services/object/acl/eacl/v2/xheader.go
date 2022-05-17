package v2

import (
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
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
	xHdrs := make([][]session.XHeader, 0)

	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		x := meta.GetXHeaders()

		ln += len(x)

		xHdrs = append(xHdrs, x)
	}

	res := make([]eaclSDK.Header, 0, ln)

	for i := range xHdrs {
		for j := range xHdrs[i] {
			res = append(res, sessionSDK.NewXHeaderFromV2(&xHdrs[i][j]))
		}
	}

	return res
}

func (s responseXHeaderSource) GetXHeaders() []eaclSDK.Header {
	ln := 0
	xHdrs := make([][]session.XHeader, 0)

	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		x := meta.GetXHeaders()

		ln += len(x)

		xHdrs = append(xHdrs, x)
	}

	res := make([]eaclSDK.Header, 0, ln)

	for i := range xHdrs {
		for j := range xHdrs[i] {
			res = append(res, sessionSDK.NewXHeaderFromV2(&xHdrs[i][j]))
		}
	}

	return res
}
