package shard

import (
	"fmt"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
)

type ContainerSizePrm struct {
	cid *cid.ID
}

type ContainerSizeRes struct {
	size uint64
}

func (p *ContainerSizePrm) WithContainerID(cid *cid.ID) *ContainerSizePrm {
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
		return nil, fmt.Errorf("could not get container size: %w", err)
	}

	return &ContainerSizeRes{
		size: size,
	}, nil
}

func ContainerSize(s *Shard, cid *cid.ID) (uint64, error) {
	res, err := s.ContainerSize(&ContainerSizePrm{cid: cid})
	if err != nil {
		return 0, err
	}

	return res.Size(), nil
}
