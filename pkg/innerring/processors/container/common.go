package container

import (
	"errors"
	"fmt"
	"time"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

var (
	errWrongSessionVerb = errors.New("wrong token verb")
	errWrongCID         = errors.New("wrong container ID")
)

type signatureVerificationData struct {
	ownerContainer user.ID

	verb   session.ContainerVerb
	verbV2 sessionv2.Verb

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
		var tokV2 sessionv2.Token
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

// verifySessionV2 validates V2 session token for container operations.
func (cp *Processor) verifySessionV2(tok sessionv2.Token, v signatureVerificationData) error {
	if err := tok.Validate(cp.resolver); err != nil {
		return fmt.Errorf("invalid V2 session token: %w", err)
	}

	if err := icrypto.AuthenticateTokenV2(&tok, historicN3ScriptRunner{
		Client:       cp.cnrClient.Morph(),
		NetworkState: cp.netState,
	}); err != nil {
		return fmt.Errorf("authenticate session token: %w", err)
	}

	if v.idContainerSet {
		if !tok.AssertContainer(v.verbV2, v.idContainer) {
			return errWrongCID
		}
	}

	if tok.OriginalIssuer() != v.ownerContainer {
		return errors.New("owner differs with original token issuer")
	}

	currentTime := cp.chainTime.Now().Round(time.Second)
	if !tok.ValidAt(currentTime) {
		return fmt.Errorf("v2 token is not valid at %s", currentTime)
	}

	return nil
}
