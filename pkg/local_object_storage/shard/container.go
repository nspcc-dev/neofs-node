package shard

import (
	"context"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

func (s *Shard) ContainerSize(cnr cid.ID) (uint64, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return 0, ErrDegradedMode
	}

	return s.metaBase.ContainerSize(cnr)
}

// DeleteContainer deletes any information related to the container
// including:
// - Metabase;
// - Blobstor;
// - Pilorama (if configured);
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
