package shard

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/pkg/errors"
)

type ContainerSizePrm struct {
	cid *container.ID
}

type ContainerSizeRes struct {
	size uint64
}

func (p *ContainerSizePrm) WithContainerID(cid *container.ID) *ContainerSizePrm {
	if p != nil {
		p.cid = cid
	}

	return p
}

func (r *ContainerSizeRes) Size() uint64 {
	return r.size
}

func (s *Shard) ContainerSize(prm *ContainerSizePrm) (*ContainerSizeRes, error) {
	size, err := s.metaBase.ContainerSize(prm.cid)
	if err != nil {
		return nil, errors.Wrap(err, "could not get container size")
	}

	return &ContainerSizeRes{
		size: size,
	}, nil
}

func ContainerSize(s *Shard, cid *container.ID) (uint64, error) {
	res, err := s.ContainerSize(&ContainerSizePrm{cid: cid})
	if err != nil {
		return 0, err
	}

	return res.Size(), nil
}
