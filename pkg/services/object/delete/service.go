package deletesvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

type Service struct {
	*cfg
}

type Option func(*cfg)

type cfg struct {
	ownerID *owner.ID

	keyStorage *objutil.KeyStorage

	putSvc *putsvc.Service

	headSvc *headsvc.Service
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

	tool := &deleteTool{
		ctx:       ctx,
		putSvc:    s.putSvc,
		obj:       newTombstone(ownerID, prm.addr.GetContainerID()),
		addr:      prm.addr,
		commonPrm: prm.common,
	}

	if err := s.deleteAll(tool); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not delete all objcet relations", s)
	}

	return new(Response), nil
}

func (s *Service) deleteAll(tool *deleteTool) error {
	headResult, err := s.headSvc.Head(tool.ctx, new(headsvc.Prm).
		WithAddress(tool.addr).
		WithCommonPrm(tool.commonPrm),
	)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not receive Head result", s)
	}

	hdr := headResult.Header()

	if err := tool.delete(hdr.GetID()); err != nil {
		return errors.Wrapf(err, "(%T) could not remove object", s)
	}

	prevID := hdr.GetPreviousID()

	if rightChild := headResult.RightChild(); rightChild != nil {
		if err := tool.delete(rightChild.GetID()); err != nil {
			return errors.Wrapf(err, "(%T) could not remove right child", s)
		}

		prevID = rightChild.GetPreviousID()
	}

	if prevID != nil {
		tool.addr.SetObjectID(prevID)

		return s.deleteAll(tool)
	}

	return nil
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
