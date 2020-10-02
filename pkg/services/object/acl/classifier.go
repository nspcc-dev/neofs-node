package acl

import (
	"bytes"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	acl "github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
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
	cnr *container.Container) acl.Role {

	if cid == nil {
		// log there
		return acl.RoleUnknown
	}

	ownerID, ownerKey, err := requestOwner(req)
	if err != nil || ownerID == nil || ownerKey == nil {
		// log there
		return acl.RoleUnknown
	}

	// todo: get owner from neofs.id if present

	// if request owner is the same as container owner, return RoleUser
	if bytes.Equal(cnr.GetOwnerID().GetValue(), ownerID.ToV2().GetValue()) {
		return acl.RoleUser
	}

	ownerKeyInBytes := crypto.MarshalPublicKey(ownerKey)

	isInnerRingNode, err := c.isInnerRingKey(ownerKeyInBytes)
	if err != nil {
		// log there
		return acl.RoleUnknown
	} else if isInnerRingNode {
		return acl.RoleSystem
	}

	isContainerNode, err := c.isContainerKey(ownerKeyInBytes, cid.ToV2().GetValue(), cnr)
	if err != nil {
		// log there
		return acl.RoleUnknown
	} else if isContainerNode {
		return acl.RoleSystem
	}

	// if none of above, return RoleOthers
	return acl.RoleOthers
}

func requestOwner(req metaWithToken) (*owner.ID, *ecdsa.PublicKey, error) {
	if req.vheader == nil {
		return nil, nil, errors.Wrap(ErrMalformedRequest, "nil verification header")
	}

	// if session token is presented, use it as truth source
	if req.token != nil {
		body := req.token.GetBody()
		if body == nil {
			return nil, nil, errors.Wrap(ErrMalformedRequest, "nil at session token body")
		}

		signature := req.token.GetSignature()
		if signature == nil {
			return nil, nil, errors.Wrap(ErrMalformedRequest, "nil at signature")
		}

		return owner.NewIDFromV2(body.GetOwnerID()), crypto.UnmarshalPublicKey(signature.GetKey()), nil
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
