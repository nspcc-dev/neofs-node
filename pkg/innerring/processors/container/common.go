package container

import (
	"errors"
	"fmt"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

var (
	errWrongSessionVerb = errors.New("wrong token verb")
	errWrongCID         = errors.New("wrong container ID")
)

type signatureVerificationData struct {
	ownerContainer user.ID

	verb session.ContainerVerb

	idContainerSet bool
	idContainer    cid.ID

	binTokenSession []byte

	verifScript []byte
	invocScript []byte
	rfc6979     bool

	signedData []byte
}

// verifySignature is a common method of Container service authentication. Asserts that:
//   - for trusted parties: session is valid (*) and issued by container owner
//   - operation data is witnessed by container owner or trusted party
//
// (*) includes:
//   - session token decodes correctly
//   - session issued and witnessed by the container owner
//   - session context corresponds to the container and verb in v
//   - session is "alive"
func (cp *Processor) verifySignature(v signatureVerificationData) error {
	var err error

	if len(v.binTokenSession) > 0 {
		var tok session.Container

		err = tok.Unmarshal(v.binTokenSession)
		if err != nil {
			return fmt.Errorf("decode session token: %w", err)
		}

		if err = icrypto.AuthenticateToken(&tok); err != nil {
			return fmt.Errorf("authenticate session token: %w", err)
		}

		if !tok.AssertVerb(v.verb) {
			return errWrongSessionVerb
		}

		if v.idContainerSet && !tok.AppliedTo(v.idContainer) {
			return errWrongCID
		}

		if !session.IssuedBy(tok, v.ownerContainer) {
			return errors.New("owner differs with token owner")
		}

		err = cp.checkTokenLifetime(tok)
		if err != nil {
			return fmt.Errorf("check session lifetime: %w", err)
		}

		if !tok.VerifySessionDataSignature(v.signedData, v.invocScript) {
			return errors.New("invalid signature calculated with session key")
		}

		return nil
	}

	if v.rfc6979 {
		return icrypto.AuthenticateContainerRequest(v.ownerContainer, v.verifScript, v.invocScript, v.signedData)
	}
	return icrypto.AuthenticateContainerRequestN3(v.ownerContainer, v.invocScript, v.verifScript, v.signedData, cp.cnrClient.Morph())
}

func (cp *Processor) checkTokenLifetime(token session.Container) error {
	curEpoch, err := cp.netState.Epoch()
	if err != nil {
		return fmt.Errorf("could not read current epoch: %w", err)
	}

	if !token.ValidAt(curEpoch) {
		return fmt.Errorf("token is not valid at %d", curEpoch)
	}

	return nil
}
