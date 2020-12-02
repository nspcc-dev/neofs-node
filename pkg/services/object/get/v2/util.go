package getsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
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
	p.SetAddress(object.NewAddressFromV2(body.GetAddress()))
	p.SetRaw(body.GetRaw())
	p.SetRemoteCallOptions(remoteCallOptionsFromMeta(meta)...)
	p.SetObjectWriter(&streamObjectWriter{stream})

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
