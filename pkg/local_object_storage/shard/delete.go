package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
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

	// FIXME: implement me

	return nil, nil
}
