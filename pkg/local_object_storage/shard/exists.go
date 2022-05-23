package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	addr oid.Address
}

// ExistsRes groups the resulting values of Exists operation.
type ExistsRes struct {
	ex bool
}

// WithAddress is an Exists option to set object checked for existence.
func (p *ExistsPrm) WithAddress(addr oid.Address) *ExistsPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Exists returns the fact that the object is in the shard.
func (p *ExistsRes) Exists() bool {
	return p.ex
}

// Exists checks if object is presented in shard.
//
// Returns any error encountered that does not allow to
// unambiguously determine the presence of an object.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been marked as removed.
func (s *Shard) Exists(prm ExistsPrm) (*ExistsRes, error) {
	exists, err := meta.Exists(s.metaBase, prm.addr)
	if err != nil {
		// If the shard is in degraded mode, try to consult blobstor directly.
		// Otherwise, just return an error.
		if s.GetMode() == ModeDegraded {
			var p blobstor.ExistsPrm
			p.SetAddress(prm.addr)

			res, bErr := s.blobStor.Exists(p)
			if bErr == nil {
				exists = res.Exists()
				s.log.Warn("metabase existence check finished with error",
					zap.Stringer("address", prm.addr),
					zap.String("error", err.Error()))
				err = nil
			}
		}
	}

	return &ExistsRes{
		ex: exists,
	}, err
}
