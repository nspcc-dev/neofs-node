package getsvc

import (
	"crypto/sha256"
	"hash"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/pkg/errors"
)

func (s *Service) toPrm(req *objectV2.GetRequest, stream objectSvc.GetObjectStream) (*getsvc.Prm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(getsvc.Prm)
	p.SetPrivateKey(key)

	body := req.GetBody()
	p.WithAddress(object.NewAddressFromV2(body.GetAddress()))
	p.WithRawFlag(body.GetRaw())
	p.SetRemoteCallOptions(remoteCallOptionsFromMeta(meta)...)
	p.SetObjectWriter(&streamObjectWriter{stream})

	return p, nil
}

func (s *Service) toRangePrm(req *objectV2.GetRangeRequest, stream objectSvc.GetObjectRangeStream) (*getsvc.RangePrm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(getsvc.RangePrm)
	p.SetPrivateKey(key)

	body := req.GetBody()
	p.WithAddress(object.NewAddressFromV2(body.GetAddress()))
	p.WithRawFlag(body.GetRaw())
	p.SetRemoteCallOptions(remoteCallOptionsFromMeta(meta)...)
	p.SetChunkWriter(&streamObjectRangeWriter{stream})
	p.SetRange(object.NewRangeFromV2(body.GetRange()))

	return p, nil
}

func (s *Service) toHashRangePrm(req *objectV2.GetRangeHashRequest) (*getsvc.RangeHashPrm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(getsvc.RangeHashPrm)
	p.SetPrivateKey(key)

	body := req.GetBody()
	p.WithAddress(object.NewAddressFromV2(body.GetAddress()))
	p.SetRemoteCallOptions(remoteCallOptionsFromMeta(meta)...)

	rngsV2 := body.GetRanges()
	rngs := make([]*object.Range, 0, len(rngsV2))

	for i := range rngsV2 {
		rngs = append(rngs, object.NewRangeFromV2(rngsV2[i]))
	}

	p.SetRangeList(rngs)

	switch t := body.GetType(); t {
	default:
		return nil, errors.Errorf("unknown checksum type %v", t)
	case refs.SHA256:
		p.SetHashGenerator(func() hash.Hash {
			return sha256.New()
		})
	case refs.TillichZemor:
		p.SetHashGenerator(func() hash.Hash {
			return tz.New()
		})
	}

	return p, nil
}

// can be shared accross all services
func remoteCallOptionsFromMeta(meta *session.RequestMetaHeader) []client.CallOption {
	xHdrs := meta.GetXHeaders()

	opts := make([]client.CallOption, 0, 3+len(xHdrs))

	opts = append(opts,
		client.WithBearer(token.NewBearerTokenFromV2(meta.GetBearerToken())),
		client.WithSession(token.NewSessionTokenFromV2(meta.GetSessionToken())),
		client.WithTTL(meta.GetTTL()-1),
	)

	for i := range xHdrs {
		opts = append(opts, client.WithXHeader(pkg.NewXHeaderFromV2(xHdrs[i])))
	}

	return opts
}

func splitInfoResponse(info *object.SplitInfo) *objectV2.GetResponse {
	resp := new(objectV2.GetResponse)

	body := new(objectV2.GetResponseBody)
	resp.SetBody(body)

	body.SetObjectPart(info.ToV2())

	return resp
}

func splitInfoRangeResponse(info *object.SplitInfo) *objectV2.GetRangeResponse {
	resp := new(objectV2.GetRangeResponse)

	body := new(objectV2.GetRangeResponseBody)
	resp.SetBody(body)

	body.SetRangePart(info.ToV2())

	return resp
}

func toHashResponse(typ refs.ChecksumType, res *getsvc.RangeHashRes) *objectV2.GetRangeHashResponse {
	resp := new(objectV2.GetRangeHashResponse)

	body := new(objectV2.GetRangeHashResponseBody)
	resp.SetBody(body)

	body.SetType(typ)
	body.SetHashList(res.Hashes())

	return resp
}
