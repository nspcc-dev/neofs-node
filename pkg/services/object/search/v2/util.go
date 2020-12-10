package searchsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

func (s *Service) toPrm(req *objectV2.SearchRequest, stream objectSvc.SearchStream) (*searchsvc.Prm, error) {
	meta := req.GetMetaHeader()

	key, err := s.keyStorage.GetKey(token.NewSessionTokenFromV2(meta.GetSessionToken()))
	if err != nil {
		return nil, err
	}

	p := new(searchsvc.Prm)
	p.SetPrivateKey(key)
	p.SetCommonParameters(commonParameters(meta))
	p.SetRemoteCallOptions(remoteCallOptionsFromMeta(meta)...)
	p.SetWriter(&streamWriter{
		stream: stream,
	})

	body := req.GetBody()
	p.WithContainerID(container.NewIDFromV2(body.GetContainerID()))
	p.WithSearchFilters(objectSDK.NewSearchFiltersFromV2(body.GetFilters()))

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

func commonParameters(meta *session.RequestMetaHeader) *util.CommonPrm {
	return new(util.CommonPrm).
		WithLocalOnly(meta.GetTTL() <= 1)
}
