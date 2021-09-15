package neofscontract

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// Cheque invokes `cheque` method of NeoFS contract.
func (x *ClientWrapper) Cheque(id []byte, user util.Uint160, amount int64, lock util.Uint160) error {
	return x.client.Cheque(id, user, amount, lock)
}

// AlphabetUpdate update list of alphabet nodes.
func (x *ClientWrapper) AlphabetUpdate(id []byte, pubs keys.PublicKeys) error {
	return x.client.AlphabetUpdate(id, pubs)
}
