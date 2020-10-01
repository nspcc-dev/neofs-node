package deletesvc

import (
	"context"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

type Service struct {
	*cfg
}

type Option func(*cfg)

type RelationHeader interface {
	HeadRelation(context.Context, *objectSDK.Address) (*object.Object, error)
}

type cfg struct {
	ownerID *owner.ID

	keyStorage *objutil.KeyStorage

	putSvc *putsvc.Service

	headSvc *headsvc.Service

	hdrLinking RelationHeader
}

func defaultCfg() *cfg {
	return new(cfg)
}

func NewService(opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

func (s *Service) Delete(ctx context.Context, prm *Prm) (*Response, error) {
	ownerID := s.ownerID
	if token := prm.common.SessionToken(); token != nil {
		ownerID = token.OwnerID()
	}

	if ownerID == nil {
		return nil, errors.Errorf("(%T) missing owner identifier", s)
	}

	addrList, err := s.getRelations(ctx, prm)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get object relations", s)
	}

	content := object.NewTombstoneContent()
	content.SetAddressList(addrList...)

	data, err := content.MarshalBinary()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not marshal tombstone content", s)
	}

	r, err := s.putSvc.Put(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not open put stream", s)
	}

	if err := r.Init(new(putsvc.PutInitPrm).
		WithObject(newTombstone(ownerID, prm.addr.GetContainerID())).
		WithCommonPrm(prm.common),
	); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not initialize tombstone stream", s)
	}

	if err := r.SendChunk(new(putsvc.PutChunkPrm).
		WithChunk(data),
	); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not send tombstone payload", s)
	}

	if _, err := r.Close(); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not close tombstone stream", s)
	}

	return new(Response), nil
}

func (s *Service) getRelations(ctx context.Context, prm *Prm) ([]*objectSDK.Address, error) {
	var res []*objectSDK.Address

	if linking, err := s.hdrLinking.HeadRelation(ctx, prm.addr); err != nil {
		cid := prm.addr.GetContainerID()

		for prev := prm.addr.GetObjectID(); prev != nil; {
			addr := objectSDK.NewAddress()
			addr.SetObjectID(prev)
			addr.SetContainerID(cid)

			headResult, err := s.headSvc.Head(ctx, new(headsvc.Prm).
				WithAddress(addr).
				WithCommonPrm(prm.common),
			)
			if err != nil {
				return nil, errors.Wrapf(err, "(%T) could not receive Head result", s)
			}

			hdr := headResult.Header()
			id := hdr.GetID()
			prev = hdr.GetPreviousID()

			if rightChild := headResult.RightChild(); rightChild != nil {
				id = rightChild.GetID()
				prev = rightChild.GetPreviousID()
			}

			addr.SetObjectID(id)

			res = append(res, addr)
		}
	} else {
		childList := linking.GetChildren()
		res = make([]*objectSDK.Address, 0, len(childList)+1)

		for i := range childList {
			addr := objectSDK.NewAddress()
			addr.SetObjectID(childList[i])
			addr.SetContainerID(prm.addr.GetContainerID())

			res = append(res, addr)
		}

		addr := objectSDK.NewAddress()
		addr.SetObjectID(linking.GetID())
		addr.SetContainerID(prm.addr.GetContainerID())

		res = append(res, addr)
	}

	return res, nil
}

func WithOwnerID(v *owner.ID) Option {
	return func(c *cfg) {
		c.ownerID = v
	}
}

func WithKeyStorage(v *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = v
	}
}

func WithPutService(v *putsvc.Service) Option {
	return func(c *cfg) {
		c.putSvc = v
	}
}

func WitHeadService(v *headsvc.Service) Option {
	return func(c *cfg) {
		c.headSvc = v
	}
}

func WithLinkingHeader(v RelationHeader) Option {
	return func(c *cfg) {
		c.hdrLinking = v
	}
}
