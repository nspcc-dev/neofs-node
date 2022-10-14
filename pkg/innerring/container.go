package innerring

import (
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/container"
	containerClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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

// containerContract is an interface of Container contract of the NeoFS Sidechain
// used by Inner Ring application. It shows how Inner Ring uses the contract.
// The type is used for test purposes only and should not be used (exist)
// otherwise.
type containerContract interface {
	// readContainer reads the container by cid.ID. Returns any error
	// encountered which prevented the container to be read completely.
	readContainer(*containerSDK.Container, cid.ID) error
}

// containerContract is a production containerContract provider.
type containerContractCore struct {
	cli *containerClient.Client
}

// init initializes the containerContract instance.
func (x *containerContractCore) init(c *containerClient.Client) {
	x.cli = c
}

func (x *containerContractCore) readContainer(cnr *containerSDK.Container, id cid.ID) error {
	res, err := containerClient.Get(x.cli, id)
	if err != nil {
		return fmt.Errorf("read container using Sidechain rpc client: %w", err)
	}

	*cnr = res.Value

	return nil
}

// containers encapsulates Container contract's interface of the NeoFS Sidechain
// and provides interface needed by the Inner Ring application.
//
// Implements container.Containers.
type containers struct {
	contract containerContract
}

// init initializes the containers instance.
func (x *containers) init(contract containerContract) {
	x.contract = contract
}

// ReadInfo reads container from the Container contract by the given cid.ID
// and writes container.Info. If the call fails or response is invalid,
// ReadInfo returns an error.
func (x *containers) ReadInfo(info *container.Info, id cid.ID) error {
	var cnr containerSDK.Container

	err := x.contract.readContainer(&cnr, id)
	if err != nil {
		return fmt.Errorf("read container from Container contract: %w", err)
	}

	info.Owner = cnr.Owner()

	if info.IsExtendableACL != nil {
		*info.IsExtendableACL = cnr.BasicACL().Extendable()
	}

	return nil
}
