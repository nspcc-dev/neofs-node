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
	if err := tok.Validate(); err != nil {
		return fmt.Errorf("invalid V2 session token: %w", err)
	}

	if !tok.VerifySignature() {
		return errors.New("v2 session token signature verification failed")
	}

	if v.idContainerSet {
		if !tok.AssertContainer(v.verbV2, v.idContainer) {
			return errWrongCID
		}
	}

	if tok.OriginalIssuer() != v.ownerContainer {
		return errors.New("owner differs with original token issuer")
	}

	currentTime, err := cp.currentChainTime()
	if err != nil {
		return fmt.Errorf("get current chain time: %w", err)
	}
	if !tok.ValidAt(currentTime) {
		return fmt.Errorf("v2 token is not valid at %s", currentTime)
	}

	return nil
}

// currentChainTime returns chain time preferring external provider.
// If provider is not set, falls back to contract latest block header.
func (cp *Processor) currentChainTime() (time.Time, error) {
	blockTimeMs, err := cp.cnrClient.Morph().MsPerBlock()
	if err != nil {
		return time.Time{}, fmt.Errorf("get FS chain block interval: %w", err)
	}
	durBlockTime := time.Duration(blockTimeMs) * time.Millisecond
	if cp.chainTime != nil {
		ct := cp.chainTime.Now()
		if !ct.IsZero() {
			return ct.Add(durBlockTime), nil
		}
		// If provider returns zero, fall back to chain.
	}

	idx, err := cp.cnrClient.Morph().BlockCount()
	if err != nil {
		return time.Time{}, fmt.Errorf("get FS chain block count: %w", err)
	}

	header, err := cp.cnrClient.Morph().GetBlockHeader(idx - 1)
	if err != nil {
		return time.Time{}, fmt.Errorf("get FS chain block header: %w", err)
	}
	return time.UnixMilli(int64(header.Timestamp)).Add(durBlockTime), nil
}
