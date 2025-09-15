package shard

import (
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
//
// If referenced object is a parent of some stored objects, Exists returns [ierrors.ErrParentObject] wrapping:
// - [*objectSDK.SplitInfoError] wrapping [objectSDK.SplitInfo] collected from stored parts;
// - [iec.ErrParts] if referenced object is EC.
func (s *Shard) Exists(addr oid.Address, ignoreExpiration bool) (bool, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		return s.blobStor.Exists(addr)
	}
	return s.metaBase.Exists(addr, ignoreExpiration)
}
