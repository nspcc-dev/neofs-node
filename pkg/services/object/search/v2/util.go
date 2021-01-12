package searchsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
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

	commonPrm, err := util.CommonPrmFromV2(req)
	if err != nil {
		return nil, err
	}

	p := new(searchsvc.Prm)
	p.SetCommonParameters(commonPrm.
		WithPrivateKey(key),
	)

	p.SetWriter(&streamWriter{
		stream: stream,
	})

	body := req.GetBody()
	p.WithContainerID(container.NewIDFromV2(body.GetContainerID()))
	p.WithSearchFilters(objectSDK.NewSearchFiltersFromV2(body.GetFilters()))

	return p, nil
}
