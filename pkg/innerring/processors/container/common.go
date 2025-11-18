package container

import (
	"errors"
	"fmt"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
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

	signedData []byte
}

type historicN3ScriptRunner struct {
	*client.Client
	NetworkState
}

// verifySignature is a common method of Container service authentication. Asserts that:
//   - for trusted parties: session is valid (*) and issued by container owner
//   - operation data is witnessed by container owner or trusted party
//
// (*) includes:
//   - session token decodes correctly (V2 or V1)
//   - session issued and witnessed by the container owner
//   - session context corresponds to the container and verb in v
//   - session is "alive"
func (cp *Processor) verifySignature(v signatureVerificationData) error {
	var err error

	if len(v.binTokenSession) > 0 {
		var tokV2 session.TokenV2
		err = tokV2.Unmarshal(v.binTokenSession)
		if err == nil {
			return cp.verifySessionV2(tokV2, v)
		}

		// Fall back to V1 token
		var tok session.Container

		err = tok.Unmarshal(v.binTokenSession)
		if err != nil {
			return fmt.Errorf("decode session token: %w", err)
		}

		if err = icrypto.AuthenticateToken(&tok, historicN3ScriptRunner{
			Client:       cp.cnrClient.Morph(),
			NetworkState: cp.netState,
		}); err != nil {
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

	return icrypto.AuthenticateContainerRequest(v.ownerContainer, v.invocScript, v.verifScript, v.signedData, cp.cnrClient.Morph())
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

func (cp *Processor) checkTokenV2Lifetime(token session.TokenV2) error {
	curEpoch, err := cp.netState.Epoch()
	if err != nil {
		return fmt.Errorf("could not read current epoch: %w", err)
	}

	if !token.ValidAt(curEpoch) {
		return fmt.Errorf("token is not valid at %d", curEpoch)
	}

	return nil
}

// verifySessionV2 validates V2 session token for container operations.
func (cp *Processor) verifySessionV2(tok session.TokenV2, v signatureVerificationData) error {
	if err := tok.Validate(); err != nil {
		return fmt.Errorf("invalid V2 session token: %w", err)
	}

	if !tok.VerifySignature() {
		return errors.New("v2 session token signature verification failed")
	}

	verbV2 := containerVerbToVerbV2(v.verb)
	if verbV2 == 0 {
		return fmt.Errorf("unsupported container verb: %v", v.verb)
	}

	if v.idContainerSet {
		if !tok.AssertContainer(verbV2, v.idContainer) {
			return errWrongCID
		}
	}

	issuer := tok.Issuer()
	if !issuer.IsOwnerID() {
		return errors.New("v2 session issuer must be OwnerID")
	}
	// TODO: make for NNS

	if issuer.OwnerID() != v.ownerContainer {
		return errors.New("owner differs with token issuer")
	}

	if err := cp.checkTokenV2Lifetime(tok); err != nil {
		return fmt.Errorf("check V2 session lifetime: %w", err)
	}

	return nil
}

// containerVerbToVerbV2 converts V1 ContainerVerb to V2 VerbV2.
func containerVerbToVerbV2(v1Verb session.ContainerVerb) session.VerbV2 {
	switch v1Verb {
	case session.VerbContainerPut:
		return session.VerbV2ContainerPut
	case session.VerbContainerDelete:
		return session.VerbV2ContainerDelete
	case session.VerbContainerSetEACL:
		return session.VerbV2ContainerSetEACL
	default:
		return 0
	}
}
