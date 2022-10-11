package container

import (
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

var (
	errWrongSessionVerb = errors.New("wrong token verb")
	errWrongCID         = errors.New("wrong container ID")
)

type signatureVerificationData struct {
	containerCreator user.ID

	verb session.ContainerVerb

	idContainerSet bool
	idContainer    cid.ID

	binTokenSession []byte

	binPublicKey []byte

	signature []byte

	signedData []byte
}

// verifySignature is a common method of Container service authentication. Asserts that:
//   - for trusted parties: session is valid (*) and issued by container owner
//   - operation data is signed by container owner or trusted party
//   - operation data signature is correct
//
// (*) includes:
//   - session token decodes correctly
//   - signature is valid
//   - session issued by the container owner
//   - v.binPublicKey is a public session key
//   - session context corresponds to the container and verb in v
//   - session is "alive"
func (x *Processor) verifySignature(v signatureVerificationData) error {
	var err error
	var key *neofsecdsa.PublicKeyRFC6979
	keyProvided := v.binPublicKey != nil

	if keyProvided {
		key = new(neofsecdsa.PublicKeyRFC6979)

		err = key.Decode(v.binPublicKey)
		if err != nil {
			return fmt.Errorf("decode public key: %w", err)
		}
	}

	if len(v.binTokenSession) > 0 {
		var tok session.Container

		err = tok.Unmarshal(v.binTokenSession)
		if err != nil {
			return fmt.Errorf("decode session token: %w", err)
		}

		if !tok.VerifySignature() {
			return errors.New("invalid session token signature")
		}

		// FIXME(@cthulhu-rider): #1387 check token is signed by container owner, see neofs-sdk-go#233

		if keyProvided && !tok.AssertAuthKey(key) {
			return errors.New("signed with a non-session key")
		}

		if !tok.AssertVerb(v.verb) {
			return errWrongSessionVerb
		}

		if v.idContainerSet && !tok.AppliedTo(v.idContainer) {
			return errWrongCID
		}

		if !session.IssuedBy(tok, v.containerCreator) {
			return errors.New("owner differs with token owner")
		}

		err = x.checkTokenLifetime(tok)
		if err != nil {
			return fmt.Errorf("check session lifetime: %w", err)
		}

		if !tok.VerifySessionDataSignature(v.signedData, v.signature) {
			return errors.New("invalid signature calculated with session key")
		}

		return nil
	}

	return x.neoFS.Authorize(v.containerCreator, v.signedData, v.signature, key)
}

func (x *Processor) checkTokenLifetime(token session.Container) error {
	curEpoch, err := x.neoFS.CurrentEpoch()
	if err != nil {
		return fmt.Errorf("could not read current epoch: %w", err)
	}

	if token.InvalidAt(curEpoch) {
		return fmt.Errorf("token is not valid at %d", curEpoch)
	}

	return nil
}
