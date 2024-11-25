package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Exists checks if object is presented in shard. ignoreExpiration flag
// allows to check for expired objects.
//
// Returns any error encountered that does not allow to
// unambiguously determine the presence of an object.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been marked as removed.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) Exists(addr oid.Address, ignoreExpiration bool) (bool, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		var p common.ExistsPrm
		p.Address = addr

		res, err := s.blobStor.Exists(p)
		if err != nil {
			return false, err
		}
		return res.Exists, nil
	}
	return s.metaBase.Exists(addr, ignoreExpiration)
}
