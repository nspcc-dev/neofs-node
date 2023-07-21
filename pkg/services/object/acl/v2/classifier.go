package v2

import (
	"bytes"

	core "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

type senderClassifier struct {
	log       *zap.Logger
	innerRing InnerRingFetcher
	netmap    core.Source
}

type classifyResult struct {
	role acl.Role
	key  []byte
}

func (c senderClassifier) classify(
	req MetaWithToken,
	idCnr cid.ID,
	cnr container.Container) (res *classifyResult, err error) {
	ownerID, ownerKey, err := req.RequestOwner()
	if err != nil {
		return nil, err
	}

	// TODO: #767 get owner from neofs.id if present

	// if request owner is the same as container owner, return RoleUser
	if ownerID.Equals(cnr.Owner()) {
		return &classifyResult{
			role: acl.RoleOwner,
			key:  ownerKey,
		}, nil
	}

	isInnerRingNode, err := c.isInnerRingKey(ownerKey)
	if err != nil {
		// do not throw error, try best case matching
		c.log.Debug("can't check if request from inner ring",
			zap.String("error", err.Error()))
	} else if isInnerRingNode {
		return &classifyResult{
			role: acl.RoleInnerRing,
			key:  ownerKey,
		}, nil
	}

	isContainerNode, err := c.isContainerKey(ownerKey, idCnr, cnr)
	if err != nil {
		// error might happen if request has `RoleOther` key and placement
		// is not possible for previous epoch, so
		// do not throw error, try best case matching
		c.log.Debug("can't check if request from container node",
			zap.String("error", err.Error()))
	} else if isContainerNode {
		return &classifyResult{
			role: acl.RoleContainer,
			key:  ownerKey,
		}, nil
	}

	// if none of above, return RoleOthers
	return &classifyResult{
		role: acl.RoleOthers,
		key:  ownerKey,
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

func (c senderClassifier) isContainerKey(
	owner []byte, idCnr cid.ID,
	cnr container.Container) (bool, error) {
	nm, err := core.GetLatestNetworkMap(c.netmap) // first check current netmap
	if err != nil {
		return false, err
	}

	in, err := lookupKeyInContainer(nm, owner, idCnr, cnr)
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

	return lookupKeyInContainer(nm, owner, idCnr, cnr)
}

func lookupKeyInContainer(
	nm *netmap.NetMap,
	owner []byte, idCnr cid.ID,
	cnr container.Container) (bool, error) {
	cnrVectors, err := nm.ContainerNodes(cnr.PlacementPolicy(), idCnr)
	if err != nil {
		return false, err
	}

	for i := range cnrVectors {
		for j := range cnrVectors[i] {
			if bytes.Equal(cnrVectors[i][j].PublicKey(), owner) {
				return true, nil
			}
		}
	}

	return false, nil
}
