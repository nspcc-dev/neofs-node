package neofscontract

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// Cheque invokes `cheque` method of NeoFS contract.
func (x *Client) Cheque(id []byte, user util.Uint160, amount int64, lock util.Uint160) error {
	return x.client.Invoke(x.chequeMethod, id, user.BytesBE(), amount, lock.BytesBE())
}

// AlphabetUpdate update list of alphabet nodes.
func (x *Client) AlphabetUpdate(id []byte, pubs keys.PublicKeys) error {
	return x.client.Invoke(x.alphabetUpdateMethod, id, pubs)
}
