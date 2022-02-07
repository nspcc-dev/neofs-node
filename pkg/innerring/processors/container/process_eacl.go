package container

import (
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"go.uber.org/zap"
)

func (cp *Processor) processSetEACL(e container.SetEACL) {
	if !cp.alphabetState.IsAlphabet() {
		cp.log.Info("non alphabet mode, ignore set EACL")
		return
	}

	err := cp.checkSetEACL(e)
	if err != nil {
		cp.log.Error("set EACL check failed",
			zap.String("error", err.Error()),
		)

		return
	}

	cp.approveSetEACL(e)
}

func (cp *Processor) checkSetEACL(e container.SetEACL) error {
	// verify signature
	key, err := keys.NewPublicKeyFromBytes(e.PublicKey(), elliptic.P256())
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	binTable := e.Table()
	tableHash := sha256.Sum256(binTable)

	if !key.Verify(e.Signature(), tableHash[:]) {
		return errors.New("invalid signature")
	}

	// verify the identity of the container owner

	// unmarshal table
	table := eacl.NewTable()

	err = table.Unmarshal(binTable)
	if err != nil {
		return fmt.Errorf("invalid binary table: %w", err)
	}

	// receive owner of the related container
	cnr, err := cntClient.Get(cp.cnrClient, table.CID())
	if err != nil {
		return fmt.Errorf("could not receive the container: %w", err)
	}

	// unmarshal session token if presented
	tok, err := tokenFromEvent(e)
	if err != nil {
		return err
	}

	if tok != nil {
		// check token context
		err = checkTokenContextWithCID(tok, table.CID(), func(c *session.ContainerContext) bool {
			return c.IsForSetEACL()
		})
		if err != nil {
			return err
		}
	}

	// statement below is a little hack, but if we write token from event to container,
	// checkKeyOwnership method will work just as it should:
	//  * tok == nil => we will check if key is a container owner's key
	//  * tok != nil => we will check if token was signed correctly (context is checked at the statement above)
	cnr.SetSessionToken(tok)

	// check key ownership
	return cp.checkKeyOwnership(cnr, key)
}

func (cp *Processor) approveSetEACL(e container.SetEACL) {
	var err error

	prm := cntClient.PutEACLPrm{}

	prm.SetTable(e.Table())
	prm.SetKey(e.PublicKey())
	prm.SetSignature(e.Signature())
	prm.SetToken(e.SessionToken())

	if nr := e.NotaryRequest(); nr != nil {
		// setEACL event was received via Notary service
		err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		// setEACL event was received via notification service
		err = cp.cnrClient.PutEACL(prm)
	}
	if err != nil {
		cp.log.Error("could not approve set EACL",
			zap.String("error", err.Error()),
		)
	}
}
