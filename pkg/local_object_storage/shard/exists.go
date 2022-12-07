package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	addr oid.Address
}

// ExistsRes groups the resulting values of Exists operation.
type ExistsRes struct {
	ex bool
}

// SetAddress is an Exists option to set object checked for existence.
func (p *ExistsPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// Exists returns the fact that the object is in the shard.
func (p ExistsRes) Exists() bool {
	return p.ex
}

// Exists checks if object is presented in shard.
//
// Returns any error encountered that does not allow to
// unambiguously determine the presence of an object.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been marked as removed.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) Exists(prm ExistsPrm) (ExistsRes, error) {
	var exists bool
	var err error

	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.NoMetabase() {
		var p common.ExistsPrm
		p.Address = prm.addr

		var res common.ExistsRes
		res, err = s.blobStor.Exists(p)
		exists = res.Exists
	} else {
		var existsPrm meta.ExistsPrm
		existsPrm.SetAddress(prm.addr)

		var res meta.ExistsRes
		res, err = s.metaBase.Exists(existsPrm)
		exists = res.Exists()
	}

	return ExistsRes{
		ex: exists,
	}, err
}
