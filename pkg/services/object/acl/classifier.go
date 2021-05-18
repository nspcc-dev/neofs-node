package acl

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	acl "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/util/signature"
	bearer "github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	v2signature "github.com/nspcc-dev/neofs-api-go/v2/signature"
	crypto "github.com/nspcc-dev/neofs-crypto"
	core "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"go.uber.org/zap"
)

type (
	InnerRingFetcher interface {
		InnerRingKeys() ([][]byte, error)
	}

	metaWithToken struct {
		vheader *session.RequestVerificationHeader
		token   *session.SessionToken
		bearer  *bearer.BearerToken
		src     interface{}
	}

	SenderClassifier struct {
		log       *zap.Logger
		innerRing InnerRingFetcher
		netmap    core.Source
	}
)

// fixme: update classifier constructor
func NewSenderClassifier(l *zap.Logger, ir InnerRingFetcher, nm core.Source) SenderClassifier {
	return SenderClassifier{
		log:       l,
		innerRing: ir,
		netmap:    nm,
	}
}

func (c SenderClassifier) Classify(
	req metaWithToken,
	cid *container.ID,
	cnr *container.Container) (role acl.Role, isIR bool, key []byte, err error) {
	if cid == nil {
		return 0, false, nil, fmt.Errorf("%w: container id is not set", ErrMalformedRequest)
	}

	ownerID, ownerKey, err := requestOwner(req)
	if err != nil {
		return 0, false, nil, err
	}

	ownerKeyInBytes := crypto.MarshalPublicKey(ownerKey)

	// todo: get owner from neofs.id if present

	// if request owner is the same as container owner, return RoleUser
	if bytes.Equal(cnr.OwnerID().ToV2().GetValue(), ownerID.ToV2().GetValue()) {
		return acl.RoleUser, false, ownerKeyInBytes, nil
	}

	isInnerRingNode, err := c.isInnerRingKey(ownerKeyInBytes)
	if err != nil {
		// do not throw error, try best case matching
		c.log.Debug("can't check if request from inner ring",
			zap.String("error", err.Error()))
	} else if isInnerRingNode {
		return acl.RoleSystem, true, ownerKeyInBytes, nil
	}

	isContainerNode, err := c.isContainerKey(ownerKeyInBytes, cid.ToV2().GetValue(), cnr)
	if err != nil {
		// error might happen if request has `RoleOther` key and placement
		// is not possible for previous epoch, so
		// do not throw error, try best case matching
		c.log.Debug("can't check if request from container node",
			zap.String("error", err.Error()))
	} else if isContainerNode {
		return acl.RoleSystem, false, ownerKeyInBytes, nil
	}

	// if none of above, return RoleOthers
	return acl.RoleOthers, false, ownerKeyInBytes, nil
}

func requestOwner(req metaWithToken) (*owner.ID, *ecdsa.PublicKey, error) {
	if req.vheader == nil {
		return nil, nil, fmt.Errorf("%w: nil verification header", ErrMalformedRequest)
	}

	// if session token is presented, use it as truth source
	if req.token.GetBody() != nil {
		// verify signature of session token
		return ownerFromToken(req.token)
	}

	// otherwise get original body signature
	bodySignature := originalBodySignature(req.vheader)
	if bodySignature == nil {
		return nil, nil, fmt.Errorf("%w: nil at body signature", ErrMalformedRequest)
	}

	key := crypto.UnmarshalPublicKey(bodySignature.Key())
	neo3wallet, err := owner.NEO3WalletFromPublicKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("can't create neo3 wallet: %w", err)
	}

	// form user from public key
	user := new(owner.ID)
	user.SetNeo3Wallet(neo3wallet)

	return user, key, nil
}

func originalBodySignature(v *session.RequestVerificationHeader) *pkg.Signature {
	if v == nil {
		return nil
	}

	for v.GetOrigin() != nil {
		v = v.GetOrigin()
	}

	return pkg.NewSignatureFromV2(v.GetBodySignature())
}

func (c SenderClassifier) isInnerRingKey(owner []byte) (bool, error) {
	innerRingKeys, err := c.innerRing.InnerRingKeys()
	if err != nil {
		return false, err
	}

	// if request owner key in the inner ring list, return RoleSystem
	for i := range innerRingKeys {
		if bytes.Equal(innerRingKeys[i], owner) {
			return true, nil
		}
	}

	return false, nil
}

func (c SenderClassifier) isContainerKey(
	owner, cid []byte,
	cnr *container.Container) (bool, error) {
	nm, err := core.GetLatestNetworkMap(c.netmap) // first check current netmap
	if err != nil {
		return false, err
	}

	in, err := lookupKeyInContainer(nm, owner, cid, cnr)
	if err != nil {
		return false, err
	} else if in {
		return true, nil
	}

	// then check previous netmap, this can happen in-between epoch change
	// when node migrates data from last epoch container
	nm, err = core.GetPreviousNetworkMap(c.netmap)
	if err != nil {
		return false, err
	}

	return lookupKeyInContainer(nm, owner, cid, cnr)
}

func lookupKeyInContainer(
	nm *netmap.Netmap,
	owner, cid []byte,
	cnr *container.Container) (bool, error) {
	cnrNodes, err := nm.GetContainerNodes(cnr.PlacementPolicy(), cid)
	if err != nil {
		return false, err
	}

	flatCnrNodes := cnrNodes.Flatten() // we need single array to iterate on
	for i := range flatCnrNodes {
		if bytes.Equal(flatCnrNodes[i].PublicKey(), owner) {
			return true, nil
		}
	}

	return false, nil
}

func ownerFromToken(token *session.SessionToken) (*owner.ID, *ecdsa.PublicKey, error) {
	// 1. First check signature of session token.
	signWrapper := v2signature.StableMarshalerWrapper{SM: token.GetBody()}
	if err := signature.VerifyDataWithSource(signWrapper, func() (key, sig []byte) {
		tokenSignature := token.GetSignature()
		return tokenSignature.GetKey(), tokenSignature.GetSign()
	}); err != nil {
		return nil, nil, fmt.Errorf("%w: invalid session token signature", ErrMalformedRequest)
	}

	// 2. Then check if session token owner issued the session token
	tokenIssuerKey := crypto.UnmarshalPublicKey(token.GetSignature().GetKey())
	tokenOwner := owner.NewIDFromV2(token.GetBody().GetOwnerID())

	if !isOwnerFromKey(tokenOwner, tokenIssuerKey) {
		// todo: in this case we can issue all owner keys from neofs.id and check once again
		return nil, nil, fmt.Errorf("%w: invalid session token owner", ErrMalformedRequest)
	}

	return tokenOwner, tokenIssuerKey, nil
}
