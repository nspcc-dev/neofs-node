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

func (p *ContainerSizePrm) SetContainerID(cnr cid.ID) {
	p.cnr = cnr
}

func (r ContainerSizeRes) Size() uint64 {
	return r.size
}

func (s *Shard) ContainerSize(prm ContainerSizePrm) (ContainerSizeRes, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return ContainerSizeRes{}, ErrDegradedMode
	}

	size, err := s.metaBase.ContainerSize(prm.cnr)
	if err != nil {
		return ContainerSizeRes{}, fmt.Errorf("could not get container size: %w", err)
	}

	return ContainerSizeRes{
		size: size,
	}, nil
}
