package v2

import (
	"bytes"
	"crypto/sha256"
	"errors"

	core "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

type senderClassifier struct {
	log       *zap.Logger
	innerRing InnerRingFetcher
	netmap    core.Source
}

type classifyResult struct {
	role eaclSDK.Role
	isIR bool
	key  []byte
}

func (c senderClassifier) classify(
	req MetaWithToken,
	idCnr cid.ID,
	cnr *container.Container) (res *classifyResult, err error) {
	ownerCnr := cnr.OwnerID()
	if ownerCnr == nil {
		return nil, errors.New("missing container owner")
	}

	ownerID, ownerKey, err := req.RequestOwner()
	if err != nil {
		return nil, err
	}

	ownerKeyInBytes := ownerKey.Bytes()

	// TODO: #767 get owner from neofs.id if present

	// if request owner is the same as container owner, return RoleUser
	if ownerID.Equals(*ownerCnr) {
		return &classifyResult{
			role: eaclSDK.RoleUser,
			isIR: false,
			key:  ownerKeyInBytes,
		}, nil
	}

	isInnerRingNode, err := c.isInnerRingKey(ownerKeyInBytes)
	if err != nil {
		// do not throw error, try best case matching
		c.log.Debug("can't check if request from inner ring",
			zap.String("error", err.Error()))
	} else if isInnerRingNode {
		return &classifyResult{
			role: eaclSDK.RoleSystem,
			isIR: true,
			key:  ownerKeyInBytes,
		}, nil
	}

	binCnr := make([]byte, sha256.Size)
	idCnr.Encode(binCnr)

	isContainerNode, err := c.isContainerKey(ownerKeyInBytes, binCnr, cnr)
	if err != nil {
		// error might happen if request has `RoleOther` key and placement
		// is not possible for previous epoch, so
		// do not throw error, try best case matching
		c.log.Debug("can't check if request from container node",
			zap.String("error", err.Error()))
	} else if isContainerNode {
		return &classifyResult{
			role: eaclSDK.RoleSystem,
			isIR: false,
			key:  ownerKeyInBytes,
		}, nil
	}

	// if none of above, return RoleOthers
	return &classifyResult{
		role: eaclSDK.RoleOthers,
		key:  ownerKeyInBytes,
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
	owner, idCnr []byte,
	cnr *container.Container) (bool, error) {
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
	owner, idCnr []byte,
	cnr *container.Container) (bool, error) {
	policy := cnr.PlacementPolicy()
	if policy == nil {
		return false, errors.New("missing placement policy in container")
	}

	cnrVectors, err := nm.ContainerNodes(*policy, idCnr)
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
