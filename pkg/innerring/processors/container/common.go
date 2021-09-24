package container

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/pkg/session"
)

var (
	errWrongSessionContext = errors.New("wrong session context")
	errWrongSessionVerb    = errors.New("wrong token verb")
	errWrongCID            = errors.New("wrong container ID")
)

type ownerIDSource interface {
	OwnerID() *owner.ID
}

func tokenFromEvent(src interface {
	SessionToken() []byte
}) (*session.Token, error) {
	binToken := src.SessionToken()

	if len(binToken) == 0 {
		return nil, nil
	}

	tok := session.NewToken()

	err := tok.Unmarshal(binToken)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal session token: %w", err)
	}

	return tok, nil
}

func (cp *Processor) checkKeyOwnership(ownerIDSrc ownerIDSource, key *keys.PublicKey) error {
	if tokenSrc, ok := ownerIDSrc.(interface {
		SessionToken() *session.Token
	}); ok {
		if token := tokenSrc.SessionToken(); token != nil {
			return cp.checkKeyOwnershipWithToken(ownerIDSrc, key, tokenSrc.SessionToken())
		}
	}

	// TODO: need more convenient way to do this
	w, err := owner.NEO3WalletFromPublicKey(&ecdsa.PublicKey{
		Curve: key.Curve,
		X:     key.X,
		Y:     key.Y,
	})
	if err != nil {
		return err
	}

	if ownerIDSrc.OwnerID().Equal(owner.NewIDFromNeo3Wallet(w)) {
		return nil
	}

	ownerKeys, err := cp.idClient.AccountKeys(ownerIDSrc.OwnerID())
	if err != nil {
		return fmt.Errorf("could not received owner keys %s: %w", ownerIDSrc.OwnerID(), err)
	}

	for _, ownerKey := range ownerKeys {
		if ownerKey.Equal(key) {
			return nil
		}
	}

	return fmt.Errorf("key %s is not tied to the owner of the container", key)
}

func (cp *Processor) checkKeyOwnershipWithToken(ownerIDSrc ownerIDSource, key *keys.PublicKey, token *session.Token) error {
	// check session key
	if !bytes.Equal(
		key.Bytes(),
		token.SessionKey(),
	) {
		return errors.New("signed with a non-session key")
	}

	// check owner
	if !token.OwnerID().Equal(ownerIDSrc.OwnerID()) {
		return errors.New("owner differs with token owner")
	}

	err := cp.checkSessionToken(token)
	if err != nil {
		return fmt.Errorf("invalid session token: %w", err)
	}

	return nil
}

func (cp *Processor) checkSessionToken(token *session.Token) error {
	// verify signature
	if !token.VerifySignature() {
		return errors.New("invalid signature")
	}

	// check lifetime
	err := cp.checkTokenLifetime(token)
	if err != nil {
		return err
	}

	// check token owner's key ownership

	key, err := keys.NewPublicKeyFromBytes(token.Signature().Key(), elliptic.P256())
	if err != nil {
		return fmt.Errorf("invalid key: %w", err)
	}

	return cp.checkKeyOwnership(token, key)
}

type verbAssert func(*session.ContainerContext) bool

func contextWithVerifiedVerb(tok *session.Token, verbAssert verbAssert) (*session.ContainerContext, error) {
	c := session.GetContainerContext(tok)
	if c == nil {
		return nil, errWrongSessionContext
	}

	if !verbAssert(c) {
		return nil, errWrongSessionVerb
	}

	return c, nil
}

func checkTokenContext(tok *session.Token, verbAssert verbAssert) error {
	_, err := contextWithVerifiedVerb(tok, verbAssert)
	return err
}

func checkTokenContextWithCID(tok *session.Token, id *cid.ID, verbAssert verbAssert) error {
	c, err := contextWithVerifiedVerb(tok, verbAssert)
	if err != nil {
		return err
	}

	tokCID := c.Container()
	if tokCID != nil && !tokCID.Equal(id) {
		return errWrongCID
	}

	return nil
}

func (cp *Processor) checkTokenLifetime(token *session.Token) error {
	curEpoch, err := cp.netState.Epoch()
	if err != nil {
		return fmt.Errorf("could not read current epoch: %w", err)
	}

	nbf := token.Nbf()
	if curEpoch < nbf {
		return fmt.Errorf("token is not valid yet: nbf %d, cur %d", nbf, curEpoch)
	}

	iat := token.Iat()
	if curEpoch < iat {
		return fmt.Errorf("token is issued in future: iat %d, cur %d", iat, curEpoch)
	}

	exp := token.Exp()
	if curEpoch >= exp {
		return fmt.Errorf("token is expired: exp %d, cur %d", exp, curEpoch)
	}

	return nil
}
