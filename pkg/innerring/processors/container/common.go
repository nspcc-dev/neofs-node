package container

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
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
func (cp *Processor) verifySignature(v signatureVerificationData) error {
	var err error
	var key neofsecdsa.PublicKeyRFC6979
	keyProvided := v.binPublicKey != nil

	if keyProvided {
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

		sig, ok := tok.Signature()
		if !ok {
			return errors.New("missing session token signature")
		}
		switch scheme := sig.Scheme(); scheme {
		default:
			return fmt.Errorf("unknown signature scheme %v", scheme)
		case neofscrypto.ECDSA_SHA512, neofscrypto.ECDSA_WALLETCONNECT:
			return fmt.Errorf("wrong signature scheme %v", scheme)
		case neofscrypto.ECDSA_DETERMINISTIC_SHA256:
			var signerPub neofsecdsa.PublicKeyRFC6979
			if err = signerPub.Decode(sig.PublicKeyBytes()); err != nil {
				return fmt.Errorf("invalid issuer public key: %w", err)
			}
			if !signerPub.Verify(tok.SignedData(), sig.Value()) {
				return errors.New("session token signature mismatch")
			}
			// TODO(@cthulhu-rider): #1387 check bound keys via NeoFSID contract?
			if user.NewFromECDSAPublicKey(ecdsa.PublicKey(signerPub)) != v.ownerContainer {
				return errors.New("session token is not signed by the container owner")
			}
		case 3: // TODO: use const after SDK upgrade
			invocScript, verifScript := sig.Value(), sig.PublicKeyBytes()
			if len(verifScript) > 0 {
				issuer := tok.Issuer()
				acc := hash.Hash160(verifScript)
				if inToken := util.Uint160(issuer[1:][:util.Uint160Size]); inToken != acc {
					return fmt.Errorf("session token's issuer account %s not equals the verification script hash %s", inToken, acc)
				}
				// TODO: scripts may be stateless like simple- or multi-signature, they do not require exec on a VM
				iatEpoch := tok.Iat()
				iatHeight, err := interface {
					HeightAtEpoch(uint64) (uint32, error)
				}(nil).HeightAtEpoch(tok.Iat())
				if err != nil {
					return fmt.Errorf("get FS chain height (session iat: epoch#%d): %w", iatEpoch, err)
				}
				blkHdr, err := interface {
					GetBlockHeaderByIndex(uint32) (block.Header, error)
				}(nil).GetBlockHeaderByIndex(iatHeight)
				if err != nil {
					return fmt.Errorf("get FS chain block header (session iat: epoch#%d, height#%d): %w", iatHeight, iatEpoch, err)
				}
				// FIXME: add signed token data to the exec context
				tx := &transaction.Transaction{
					Script:  slices.Concat(invocScript, verifScript),
					Signers: []transaction.Signer{{Account: acc}},
					// FIXME: what else?
				}
				// w8 4 https://github.com/nspcc-dev/neo-go/issues/3836
				ok, err := unwrap.Bool(cp.cnrClient.Morph().InvokeContainedScript(tx, blkHdr, trigger.Verification))
				if err != nil {
					return fmt.Errorf("invoke contained auth script on FS chain (session iat: epoch#%d, height#%d): %w", iatHeight, iatEpoch, err)
				}
				if !ok {
					return fmt.Errorf("auth script run on FS chain resulted in false (session iat: epoch#%d, height#%d)", iatHeight, iatEpoch)
				}
			} else { // FIXME: what?
			}
		}

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

		// TODO: support N3 accounts as well
		if !tok.VerifySessionDataSignature(v.signedData, v.signature) {
			return errors.New("invalid signature calculated with session key")
		}

		return nil
	}

	if keyProvided {
		// TODO(@cthulhu-rider): #1387 use another approach after neofs-sdk-go#233
		idFromKey := user.NewFromECDSAPublicKey(ecdsa.PublicKey(key))

		if v.ownerContainer == idFromKey {
			if key.Verify(v.signedData, v.signature) {
				return nil
			}

			return errors.New("invalid signature calculated by container owner's key")
		}
	} else {
		var prm neofsid.AccountKeysPrm
		prm.SetID(v.ownerContainer)

		ownerKeys, err := cp.idClient.AccountKeys(prm)
		if err != nil {
			return fmt.Errorf("receive owner keys %s: %w", v.ownerContainer, err)
		}

		for i := range ownerKeys {
			if (*neofsecdsa.PublicKeyRFC6979)(ownerKeys[i]).Verify(v.signedData, v.signature) {
				return nil
			}
		}
	}

	return errors.New("signature is invalid or calculated with the key not bound to the container owner")
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
