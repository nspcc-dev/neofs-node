package shard

import (
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

type ContainerSizePrm struct {
	cnr cid.ID
}

type ContainerSizeRes struct {
	size uint64
}

func (p *ContainerSizePrm) WithContainerID(cnr cid.ID) {
	if p != nil {
		p.cnr = cnr
	}
}

func (r ContainerSizeRes) Size() uint64 {
	return r.size
}

func (s *Shard) ContainerSize(prm ContainerSizePrm) (ContainerSizeRes, error) {
	size, err := s.metaBase.ContainerSize(prm.cnr)
	if err != nil {
		return ContainerSizeRes{}, fmt.Errorf("could not get container size: %w", err)
	}

	return ContainerSizeRes{
		size: size,
	}, nil
}

func ContainerSize(s *Shard, cnr cid.ID) (uint64, error) {
	res, err := s.ContainerSize(ContainerSizePrm{cnr: cnr})
	if err != nil {
		return 0, err
	}

	return res.Size(), nil
}
