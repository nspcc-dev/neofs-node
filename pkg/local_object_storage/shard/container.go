package shard

import (
	"context"
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

func (s *Shard) ContainerInfo(cnr cid.ID) (meta.ContainerInfo, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return meta.ContainerInfo{}, ErrDegradedMode
	}

	return s.metaBase.GetContainerInfo(cnr)
}

// DeleteContainer deletes any information related to the container
// including:
// - Metabase;
// - Blobstor;
// - Write-cache (if configured).
func (s *Shard) DeleteContainer(_ context.Context, cID cid.ID) error {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ErrReadOnlyMode
	}

	inhumedAvailable, err := s.metaBase.InhumeContainer(cID)
	if err != nil {
		return fmt.Errorf("inhuming container in metabase: %w", err)
	}

	s.decObjectCounterBy(logical, inhumedAvailable)

	return nil
}
