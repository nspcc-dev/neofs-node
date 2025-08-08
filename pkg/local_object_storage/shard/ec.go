package shard

import (
	"fmt"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// TODO: docs.
// TODO: keep in sync with https://github.com/nspcc-dev/neofs-node/pull/3466.
func (s *Shard) GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (objectSDK.Object, error) {
	// FIXME: metabase can be disabled
	partID, err := s.metaBase.ResolveECPart(cnr, parent, pi)
	if err != nil {
		return objectSDK.Object{}, fmt.Errorf("resolve part ID in metabase: %w", err)
	}

	partAddr := oid.NewAddress(cnr, partID)
	if s.hasWriteCache() {
		obj, err := s.writeCache.Get(partAddr)
		if err == nil {
			return *obj, nil
		}

		// TODO: debug if 404
		s.log.Info("failed to get EC part from write-cache, trying BLOB storage...", zap.Stringer("partAddr", partAddr), zap.Error(err))
	}

	obj, err := s.blobStor.Get(partAddr)
	if err != nil {
		return objectSDK.Object{}, fmt.Errorf("get from BLOB storage by ID %s: %w", partID, err)
	}

	return *obj, nil
}
