package shard

import (
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	addr *addressSDK.Address
}

// ExistsRes groups resulting values of Exists operation.
type ExistsRes struct {
	ex bool
}

// WithAddress is an Exists option to set object checked for existence.
func (p *ExistsPrm) WithAddress(addr *addressSDK.Address) *ExistsPrm {
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
func (s *Shard) Exists(prm *ExistsPrm) (*ExistsRes, error) {
	exists, err := s.objectExists(prm.addr)

	return &ExistsRes{
		ex: exists,
	}, err
}

func (s *Shard) objectExists(addr *addressSDK.Address) (bool, error) {
	return meta.Exists(s.metaBase, addr)
}
