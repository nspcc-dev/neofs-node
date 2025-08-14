package shard

import (
	"errors"
	"fmt"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// TODO: docs. See StorageEngine caller to cover all cases.
// TODO: keep in sync with https://github.com/nspcc-dev/neofs-node/pull/3466.
func (s *Shard) GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, error) {
	partID, err := s.metaBaseIface.ResolveECPart(cnr, parent, pi)
	if err != nil {
		if errors.Is(err, meta.ErrDegradedMode) {
			return object.Object{}, ErrDegradedMode
		}
		return object.Object{}, fmt.Errorf("resolve part ID in metabase: %w", err)
	}

	partAddr := oid.NewAddress(cnr, partID)
	if s.hasWriteCache() {
		obj, err := s.writeCache.Get(partAddr)
		if err == nil {
			return *obj, nil
		}

		if errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Debug("EC part object is missing in write-cache, trying BLOB storage...", zap.Stringer("partAddr", partAddr), zap.Error(err))
		} else {
			s.log.Info("failed to get EC part object from write-cache, trying BLOB storage...", zap.Stringer("partAddr", partAddr), zap.Error(err))
		}
	}

	obj, err := s.blobStor.Get(partAddr)
	if err != nil {
		return object.Object{}, fmt.Errorf("get from BLOB storage by ID %s: %w", partID, err)
	}

	return *obj, nil
}
