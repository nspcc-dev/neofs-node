package container

import (
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/session"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/container"
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
	cnr, err := wrapper.Get(cp.cnrClient, table.CID())
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

	// check key ownership
	return cp.checkKeyOwnership(cnr, key)
}

func (cp *Processor) approveSetEACL(e container.SetEACL) {
	var err error

	if nr := e.NotaryRequest(); nr != nil {
		// setEACL event was received via Notary service
		err = cp.cnrClient.Morph().NotarySignAndInvokeTX(nr.MainTransaction)
	} else {
		// setEACL event was received via notification service
		err = cp.cnrClient.PutEACL(e.Table(), e.PublicKey(), e.Signature(), e.SessionToken())
	}
	if err != nil {
		cp.log.Error("could not approve set EACL",
			zap.String("error", err.Error()),
		)
	}
}
