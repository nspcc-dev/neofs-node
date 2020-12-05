package deletesvc

import (
	"context"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Service struct {
	*cfg
}

type Option func(*cfg)

type RelationHeader interface {
	HeadRelation(context.Context, *objectSDK.Address, *objutil.CommonPrm) (*object.Object, error)
}

type cfg struct {
	ownerID *owner.ID

	keyStorage *objutil.KeyStorage

	putSvc *putsvc.Service

	headSvc *headsvc.Service

	hdrLinking RelationHeader

	log *logger.Logger
}

func defaultCfg() *cfg {
	return &cfg{
		log: zap.L(),
	}
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

	// `WithoutSuccessTracking` option broadcast message to all container nodes.
	// For now there is no better solution to distributed tombstones with
	// content address storage (CAS) and one tombstone for several split
	// objects.
	if err := r.Init(new(putsvc.PutInitPrm).
		WithObject(newTombstone(ownerID, prm.addr.ContainerID())).
		WithCommonPrm(prm.common).
		WithTraverseOption(placement.WithoutSuccessTracking()), // broadcast tombstone, maybe one
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

	if linking, err := s.hdrLinking.HeadRelation(ctx, prm.addr, prm.common); err != nil {
		cid := prm.addr.ContainerID()

		for prev := prm.addr.ObjectID(); prev != nil; {
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
			id := hdr.ID()
			prev = hdr.PreviousID()

			addr.SetObjectID(id)

			res = append(res, addr)
		}
	} else {
		childList := linking.Children()
		res = make([]*objectSDK.Address, 0, len(childList)+2) // 1 for parent, 1 for linking

		for i := range childList {
			addr := objectSDK.NewAddress()
			addr.SetObjectID(childList[i])
			addr.SetContainerID(prm.addr.ContainerID())

			res = append(res, addr)
		}

		addr := objectSDK.NewAddress()
		addr.SetObjectID(linking.ID())
		addr.SetContainerID(prm.addr.ContainerID())

		res = append(res, addr)
	}

	res = append(res, prm.addr)

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

func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}
