package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr *objectSDK.Address
}

// DeleteRes groups resulting values of Delete operation.
type DeleteRes struct{}

// WithAddress is a Delete option to set the address of the object to delete.
//
// Option is required.
func (p *DeletePrm) WithAddress(addr *objectSDK.Address) *DeletePrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Delete marks object to delete from shard.
//
// Returns any error encountered that did not allow to completely
// mark the object to delete.
func (s *Shard) Delete(prm *DeletePrm) (*DeleteRes, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// mark object to delete in metabase
	if err := s.metaBase.Delete(prm.addr); err != nil {
		s.log.Warn("could not mark object to delete in metabase",
			zap.String("error", err.Error()),
		)
	}

	if _, err := s.blobStor.Delete(
		new(blobstor.DeletePrm).
			WithAddress(prm.addr),
	); err != nil {
		return nil, errors.Wrap(err, "could not remove object from BLOB storage")
	}

	return nil, nil
}
