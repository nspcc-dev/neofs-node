package container

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
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

	binPublicKey []byte

	signature []byte

	signedData []byte
}

// verifySignature is a common method of Container service authentication. Asserts that:
//  - for trusted parties: session is valid (*) and issued by container owner
//  - operation data is signed by container owner or trusted party
//  - operation data signature is correct
//
// (*) includes:
//  - session token decodes correctly
//  - signature is valid
//  - session issued by the container owner
//  - v.binPublicKey is a public session key
//  - session context corresponds to the container and verb in v
//  - session is "alive"
func (cp *Processor) verifySignature(v signatureVerificationData) error {
	var err error
	var key neofsecdsa.PublicKeyRFC6979
	keyProvided := v.binPublicKey != nil
	withSession := len(v.binTokenSession) > 0

	if keyProvided {
		err = key.Decode(v.binPublicKey)
		if err != nil {
			return fmt.Errorf("decode public key: %w", err)
		}
	}

	if withSession {
		var tok session.Container

		err = tok.Unmarshal(v.binTokenSession)
		if err != nil {
			return fmt.Errorf("decode session token: %w", err)
		}

		if !tok.VerifySignature() {
			return errors.New("invalid session token signature")
		}

		// FIXME(@cthulhu-rider): #1387 check token is signed by container owner, see neofs-sdk-go#233
		//  We'll get container owner's keys which is needed below, so it's worth to cache them

		if keyProvided && !tok.AssertAuthKey(&key) {
			return errors.New("signed with a non-session key")
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
	}

	var verificationKeys []neofscrypto.PublicKey

	if keyProvided {
		if withSession {
			verificationKeys = []neofscrypto.PublicKey{&key}
		} else {
			var idFromKey user.ID
			user.IDFromKey(&idFromKey, (ecdsa.PublicKey)(key))

			if v.ownerContainer.Equals(idFromKey) {
				verificationKeys = []neofscrypto.PublicKey{&key}
			}
		}
	}

	if verificationKeys == nil {
		var prm neofsid.AccountKeysPrm
		prm.SetID(v.ownerContainer)

		ownerKeys, err := cp.idClient.AccountKeys(prm)
		if err != nil {
			return fmt.Errorf("receive owner keys %s: %w", v.ownerContainer, err)
		}

		if !keyProvided {
			verificationKeys = make([]neofscrypto.PublicKey, 0, len(ownerKeys))
		}

		for i := range ownerKeys {
			if keyProvided {
				// TODO(@cthulhu-rider): keys have been decoded in order to encode only, should be optimized by #1387
				if bytes.Equal(ownerKeys[i].Bytes(), v.binPublicKey) {
					verificationKeys = []neofscrypto.PublicKey{(*neofsecdsa.PublicKeyRFC6979)(ownerKeys[i])}
					break
				}
			} else {
				verificationKeys = append(verificationKeys, (*neofsecdsa.PublicKeyRFC6979)(ownerKeys[i]))
			}
		}
	}

	if len(verificationKeys) == 0 {
		return errors.New("key is not a container owner's key")
	}

	for i := range verificationKeys {
		if verificationKeys[i].Verify(v.signedData, v.signature) {
			return nil
		}
	}

	return errors.New("invalid signature")
}

func (cp *Processor) checkTokenLifetime(token session.Container) error {
	curEpoch, err := cp.netState.Epoch()
	if err != nil {
		return fmt.Errorf("could not read current epoch: %w", err)
	}

	if token.InvalidAt(curEpoch) {
		return fmt.Errorf("token is not valid at %d", curEpoch)
	}

	return nil
}
