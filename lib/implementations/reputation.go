package implementations

import (
	"github.com/nspcc-dev/neofs-node/lib/peers"
)

// MorphReputationContract is a wrapper over NeoFS Reputation contract client
// that provides an interface of the storage of global trust values.
type MorphReputationContract struct {
	// NeoFS Reputation smart-contract
	repContract StaticContractClient

	// put method name of reputation contract
	putMethodName string

	// list method name of reputation contract
	listMethodName string

	// public key storage
	pkStore peers.PublicKeyStore
}

// SetReputationContractClient is a Reputation contract client setter.
func (s *MorphReputationContract) SetReputationContractClient(v StaticContractClient) {
	s.repContract = v
}

// SetPublicKeyStore is a public key store setter.
func (s *MorphReputationContract) SetPublicKeyStore(v peers.PublicKeyStore) {
	s.pkStore = v
}

// SetPutMethodName is a Reputation contract Put method name setter.
func (s *MorphReputationContract) SetPutMethodName(v string) {
	s.putMethodName = v
}

// SetListMethodName is a Reputation contract List method name setter.
func (s *MorphReputationContract) SetListMethodName(v string) {
	s.listMethodName = v
}
