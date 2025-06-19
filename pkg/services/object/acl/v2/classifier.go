package v2

import (
	"bytes"

	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

type senderClassifier struct {
	log       *zap.Logger
	innerRing InnerRingFetcher
	fsChain   FSChain
}

type historicN3ScriptRunner struct {
	FSChain
	Netmapper
}

func (c senderClassifier) classify(idCnr cid.ID, cnrOwner, reqAuthor user.ID, reqAuthorPub []byte) (acl.Role, error) {
	l := c.log.With(zap.Stringer("cid", idCnr), zap.Stringer("requester", reqAuthor))

	// if request owner is the same as container owner, return RoleUser
	if reqAuthor == cnrOwner {
		return acl.RoleOwner, nil
	}

	isInnerRingNode, err := c.isInnerRingKey(reqAuthorPub)
	if err != nil {
		// do not throw error, try best case matching
		l.Debug("can't check if request from inner ring",
			zap.Error(err))
	} else if isInnerRingNode {
		return acl.RoleInnerRing, nil
	}

	isContainerNode, err := c.fsChain.InContainerInLastTwoEpochs(idCnr, reqAuthorPub)
	if err != nil {
		// error might happen if request has `RoleOther` key and placement
		// is not possible for previous epoch, so
		// do not throw error, try best case matching
		l.Debug("can't check if request from container node",
			zap.Error(err))
	} else if isContainerNode {
		return acl.RoleContainer, nil
	}

	// if none of above, return RoleOthers
	return acl.RoleOthers, nil
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
