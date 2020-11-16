package headsvc

import (
	"context"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

type RelationHeader struct {
	srch RelationSearcher

	svc *Service
}

func NewRelationHeader(srch RelationSearcher, svc *Service) *RelationHeader {
	return &RelationHeader{
		srch: srch,
		svc:  svc,
	}
}

func (h *RelationHeader) HeadRelation(ctx context.Context, addr *objectSDK.Address, prm *objutil.CommonPrm) (*object.Object, error) {
	id, err := h.srch.SearchRelation(ctx, addr, prm)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not find relation", h)
	}

	a := objectSDK.NewAddress()
	a.SetContainerID(addr.ContainerID())
	a.SetObjectID(id)

	r, err := h.svc.Head(ctx, new(Prm).
		WithAddress(a).
		WithCommonPrm(prm),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive relation header", h)
	}

	return r.Header(), nil
}
