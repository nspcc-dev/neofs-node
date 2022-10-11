package innerring

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// neoFSIDContract is an interface of NeoFSID contract of the NeoFS Sidechain
// used by Inner Ring application. It shows how Inner Ring uses the contract.
// The type is used for test purposes only and should not be used (exist)
// otherwise.
type neoFSIDContract interface {
	// iterates over all public keys of the user identified by the given user.ID
	// and passes them into f. Immediately stops on f's false return without
	// an error.
	iterateUserKeys(usr user.ID, f func(neofscrypto.PublicKey) bool) error
}

// neoFSIDContractCore is a production neoFSIDContract provider.
type neoFSIDContractCore neofsid.Client

func (x *neoFSIDContractCore) iterateUserKeys(usr user.ID, f func(neofscrypto.PublicKey) bool) error {
	var prm neofsid.AccountKeysPrm
	prm.SetID(usr)

	keys, err := (*neofsid.Client)(x).AccountKeys(prm)
	if err != nil {
		return fmt.Errorf("receive user keys %s: %w", usr, err)
	}

	for i := range keys {
		if !f((*neofsecdsa.PublicKeyRFC6979)(keys[i])) {
			return nil
		}
	}

	return nil
}
