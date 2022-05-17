package v2

import (
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

	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		ln += len(meta.GetXHeaders())
	}

	res := make([]eaclSDK.Header, 0, ln)
	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		x := meta.GetXHeaders()
		for i := range x {
			res = append(res, sessionSDK.NewXHeaderFromV2(&x[i]))
		}
	}

	return res
}

func (s responseXHeaderSource) GetXHeaders() []eaclSDK.Header {
	ln := 0

	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		ln += len(meta.GetXHeaders())
	}

	res := make([]eaclSDK.Header, 0, ln)
	for meta := s.req.GetMetaHeader(); meta != nil; meta = meta.GetOrigin() {
		x := meta.GetXHeaders()
		for i := range x {
			res = append(res, sessionSDK.NewXHeaderFromV2(&x[i]))
		}
	}

	return res
}
