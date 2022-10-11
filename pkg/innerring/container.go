package innerring

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// authSystem encapsulates NeoFSID contract's interface of the NeoFS Sidechain
// and provides interface needed by the Inner Ring application.
//
// Implements container.AuthSystem.
type authSystem struct {
	neofsID neoFSIDContract
}

// init initializes the authSystem instance.
func (x *authSystem) init(neofsID neoFSIDContract) {
	x.neofsID = neofsID
}

var errIncorrectSignature = errors.New("incorrect signature")

// VerifySignature checks if provided key belongs to the given user via
// underlying NeoFSID contract client and verifies data signature. If key is
// not provided, key is immediately checked using resolving algorithm of Neo.
func (x *authSystem) VerifySignature(usr user.ID, data []byte, signature []byte, key *neofsecdsa.PublicKeyRFC6979) error {
	if key != nil {
		// TODO(@cthulhu-rider): #1387 use another approach after neofs-sdk-go#233
		var idFromKey user.ID
		user.IDFromKey(&idFromKey, ecdsa.PublicKey(*key))

		if usr.Equals(idFromKey) {
			if key.Verify(data, signature) {
				return nil
			}

			return errIncorrectSignature
		}
	}

	var valid bool

	err := x.neofsID.iterateUserKeys(usr, func(key neofscrypto.PublicKey) bool {
		valid = key.Verify(data, signature)
		return !valid
	})
	if err != nil {
		return fmt.Errorf("iterate user keys: %w", err)
	}

	if !valid {
		return errIncorrectSignature
	}

	return nil
}
