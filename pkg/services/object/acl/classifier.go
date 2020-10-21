package acl

import (
	"bytes"
	"crypto/ecdsa"

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
	"github.com/pkg/errors"
)

type (
	InnerRingFetcher interface {
		InnerRingKeys() ([][]byte, error)
	}

	metaWithToken struct {
		vheader *session.RequestVerificationHeader
		token   *session.SessionToken
		bearer  *bearer.BearerToken
	}

	SenderClassifier struct {
		innerRing InnerRingFetcher
		netmap    core.Source
	}
)

// fixme: update classifier constructor
func NewSenderClassifier(ir InnerRingFetcher, nm core.Source) SenderClassifier {
	return SenderClassifier{
		innerRing: ir,
		netmap:    nm,
	}
}

func (c SenderClassifier) Classify(
	req metaWithToken,
	cid *container.ID,
	cnr *container.Container) (acl.Role, []byte, error) {

	if cid == nil {
		return 0, nil, errors.Wrap(ErrMalformedRequest, "container id is not set")
	}

	ownerID, ownerKey, err := requestOwner(req)
	if err != nil {
		return 0, nil, err
	}

	ownerKeyInBytes := crypto.MarshalPublicKey(ownerKey)

	// todo: get owner from neofs.id if present

	// if request owner is the same as container owner, return RoleUser
	if bytes.Equal(cnr.GetOwnerID().GetValue(), ownerID.ToV2().GetValue()) {
		return acl.RoleUser, ownerKeyInBytes, nil
	}

	isInnerRingNode, err := c.isInnerRingKey(ownerKeyInBytes)
	if err != nil {
		return 0, nil, errors.Wrap(err, "can't check if request from inner ring")
	} else if isInnerRingNode {
		return acl.RoleSystem, ownerKeyInBytes, nil
	}

	isContainerNode, err := c.isContainerKey(ownerKeyInBytes, cid.ToV2().GetValue(), cnr)
	if err != nil {
		return 0, nil, errors.Wrap(err, "can't check if request from container node")
	} else if isContainerNode {
		return acl.RoleSystem, ownerKeyInBytes, nil
	}

	// if none of above, return RoleOthers
	return acl.RoleOthers, ownerKeyInBytes, nil
}

func requestOwner(req metaWithToken) (*owner.ID, *ecdsa.PublicKey, error) {
	if req.vheader == nil {
		return nil, nil, errors.Wrap(ErrMalformedRequest, "nil verification header")
	}

	// if session token is presented, use it as truth source
	if req.token.GetBody() != nil {
		// verify signature of session token
		return ownerFromToken(req.token)
	}

	// otherwise get original body signature
	bodySignature := originalBodySignature(req.vheader)
	if bodySignature == nil {
		return nil, nil, errors.Wrap(ErrMalformedRequest, "nil at body signature")
	}

	key := crypto.UnmarshalPublicKey(bodySignature.GetKey())
	neo3wallet, err := owner.NEO3WalletFromPublicKey(key)
	if err != nil {
		return nil, nil, errors.Wrap(err, "can't create neo3 wallet")
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

	// first check current netmap
	nm, err := core.GetLatestNetworkMap(c.netmap)
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

	cnrNodes, err := nm.GetContainerNodes(cnr.GetPlacementPolicy(), cid)
	if err != nil {
		return false, err
	}

	flatCnrNodes := cnrNodes.Flatten() // we need single array to iterate on
	for i := range flatCnrNodes {
		if bytes.Equal(flatCnrNodes[i].InfoV2.GetPublicKey(), owner) {
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
		return nil, nil, errors.Wrap(ErrMalformedRequest, "invalid session token signature")
	}

	// 2. Then check if session token owner issued the session token
	tokenIssuerKey := crypto.UnmarshalPublicKey(token.GetSignature().GetKey())
	tokenIssuerWallet, err := owner.NEO3WalletFromPublicKey(tokenIssuerKey)
	if err != nil {
		return nil, nil, errors.Wrap(ErrMalformedRequest, "invalid token issuer key")
	}

	if !bytes.Equal(token.GetBody().GetOwnerID().GetValue(), tokenIssuerWallet.Bytes()) {
		// todo: in this case we can issue all owner keys from neofs.id and check once again
		return nil, nil, errors.Wrap(ErrMalformedRequest, "invalid session token owner")
	}

	return owner.NewIDFromV2(token.GetBody().GetOwnerID()), tokenIssuerKey, nil
}
