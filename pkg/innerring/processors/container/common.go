package container

import (
	"errors"
	"fmt"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
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

		if issuer := tok.Issuer(); issuer != v.ownerContainer {
			return fmt.Errorf("owner %s differs with token owner %s", v.ownerContainer, issuer)
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

func skipZeroCIDLogField(cnr cid.ID) zap.Field {
	if cnr.IsZero() {
		return zap.Skip()
	}
	return zap.Stringer("cid", cnr)
}
