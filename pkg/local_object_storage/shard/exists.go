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
	ex   bool
	meta bool
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

func (p *ExistsRes) FromMeta() bool {
	return p.meta
}

// Exists checks if object is presented in shard.
//
// Returns any error encountered that does not allow to
// unambiguously determine the presence of an object.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been marked as removed.
func (s *Shard) Exists(prm ExistsPrm) (*ExistsRes, error) {
	var exists bool
	var err error

	mode := s.GetMode()
	if mode&ModeDegraded == 0 { // In Degraded mode skip metabase consulting.
		exists, err = meta.Exists(s.metaBase, prm.addr)
	}

	metaErr := err != nil
	if err != nil && mode&ModeDegraded != 0 {

		var p blobstor.ExistsPrm
		p.SetAddress(prm.addr)

		res, bErr := s.blobStor.Exists(p)
		if bErr == nil {
			exists = res.Exists()
			if err != nil {
				s.log.Warn("metabase existence check finished with error",
					zap.Stringer("address", prm.addr),
					zap.String("error", err.Error()))
			}
			err = nil
		} else if err == nil {
			err = bErr
		}
	}

	return &ExistsRes{
		ex:   exists,
		meta: metaErr,
	}, err
}
