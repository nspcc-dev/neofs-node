package neofscontract

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Cheque invokes `cheque` method of NeoFS contract.
func (x *Client) Cheque(id []byte, user util.Uint160, amount int64, lock util.Uint160) error {
	prm := client.InvokePrm{}

	prm.SetMethod(x.chequeMethod)
	prm.SetArgs(id, user.BytesBE(), amount, lock.BytesBE())

	return x.client.Invoke(prm)
}

// AlphabetUpdate update list of alphabet nodes.
func (x *Client) AlphabetUpdate(id []byte, pubs keys.PublicKeys) error {
	prm := client.InvokePrm{}

	prm.SetMethod(x.alphabetUpdateMethod)
	prm.SetArgs(id, pubs)

	return x.client.Invoke(prm)
}
