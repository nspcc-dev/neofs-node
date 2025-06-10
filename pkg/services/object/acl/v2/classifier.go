package v2

import (
	"bytes"
	"fmt"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

type senderClassifier struct {
	log       *zap.Logger
	innerRing InnerRingFetcher
	netmap    Netmapper
	fsChain   FSChain
}

type classifyResult struct {
	role    acl.Role
	key     []byte
	account *user.ID
}

type historicN3ScriptRunner struct {
	FSChain
	Netmapper
}

func (c senderClassifier) classify(
	req MetaWithToken,
	idCnr cid.ID,
	cnr container.Container,
) (res *classifyResult, err error) {
	var ownerID *user.ID
	var ownerKey []byte

	if req.token != nil {
		if err := icrypto.AuthenticateToken(req.token, historicN3ScriptRunner{
			FSChain:   c.fsChain,
			Netmapper: c.netmap,
		}); err != nil {
			return nil, fmt.Errorf("authenticate session token: %w", err)
		}
		tokenIssuer := req.token.Issuer()
		ownerID = &tokenIssuer
		ownerKey = req.token.IssuerPublicKeyBytes()
	} else {
		var idSender user.ID
		if idSender, ownerKey, err = icrypto.GetRequestAuthor(req.vheader); err != nil {
			return nil, err
		}
		ownerID = &idSender
	}

	l := c.log.With(zap.Stringer("cid", idCnr), zap.Stringer("requester", ownerID))

	// TODO: #767 get owner from neofs.id if present

	// if request owner is the same as container owner, return RoleUser
	if *ownerID == cnr.Owner() {
		return &classifyResult{
			role:    acl.RoleOwner,
			key:     ownerKey,
			account: ownerID,
		}, nil
	}

	isInnerRingNode, err := c.isInnerRingKey(ownerKey)
	if err != nil {
		// do not throw error, try best case matching
		l.Debug("can't check if request from inner ring",
			zap.Error(err))
	} else if isInnerRingNode {
		return &classifyResult{
			role:    acl.RoleInnerRing,
			key:     ownerKey,
			account: ownerID,
		}, nil
	}

	isContainerNode, err := c.fsChain.InContainerInLastTwoEpochs(idCnr, ownerKey)
	if err != nil {
		// error might happen if request has `RoleOther` key and placement
		// is not possible for previous epoch, so
		// do not throw error, try best case matching
		l.Debug("can't check if request from container node",
			zap.Error(err))
	} else if isContainerNode {
		return &classifyResult{
			role:    acl.RoleContainer,
			key:     ownerKey,
			account: ownerID,
		}, nil
	}

	// if none of above, return RoleOthers
	return &classifyResult{
		role:    acl.RoleOthers,
		key:     ownerKey,
		account: ownerID,
	}, nil
}

func (c senderClassifier) isInnerRingKey(owner []byte) (bool, error) {
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
