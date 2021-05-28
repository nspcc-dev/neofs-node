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
	"github.com/nspcc-dev/neofs-api-go/util/signature"
	signature2 "github.com/nspcc-dev/neofs-api-go/v2/signature"
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
		X: key.X,
		Y: key.Y,
	})
	if err != nil {
		return err
	}

	// TODO: need Equal method on owner.ID
	if ownerIDSrc.OwnerID().String() == owner.NewIDFromNeo3Wallet(w).String() {
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
	// TODO: need Equal method on owner.ID
	if token.OwnerID().String() != ownerIDSrc.OwnerID().String() {
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

	// TODO: need more convenient way to do this
	//  e.g. provide VerifySignature method from Token

	// FIXME: do all so as not to deepen in the version
	tokenV2 := token.ToV2()

	signWrapper := signature2.StableMarshalerWrapper{
		SM: tokenV2.GetBody(),
	}
	if err := signature.VerifyDataWithSource(signWrapper, func() (key, sig []byte) {
		tokenSignature := tokenV2.GetSignature()
		return tokenSignature.GetKey(), tokenSignature.GetSign()
	}); err != nil {
		return errors.New("invalid signature")
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
