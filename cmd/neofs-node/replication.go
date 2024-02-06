package main

import (
	"bytes"
	"fmt"

	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// replicationNode checks object storage policy compliance against NeoFS network
// maps.
type replicationNode struct {
	log *zap.Logger

	putObjectService *putsvc.Service

	getPubKey                 func() []byte
	getContainerStoragePolicy func(cid.ID) (netmapsdk.PlacementPolicy, error)
	getCurrentEpoch           func() uint64
	getNetmap                 func(epoch uint64) (netmapsdk.NetMap, error)
}

func newReplicationNode(
	log *zap.Logger,
	putObjectService *putsvc.Service,
	getLocalNodePubKey func() []byte,
	getContainerStoragePolicy func(cid.ID) (netmapsdk.PlacementPolicy, error),
	getCurrentEpoch func() uint64,
	getNetmap func(epoch uint64) (netmapsdk.NetMap, error),
) *replicationNode {
	return &replicationNode{
		log:                       log,
		putObjectService:          putObjectService,
		getPubKey:                 getLocalNodePubKey,
		getContainerStoragePolicy: getContainerStoragePolicy,
		getCurrentEpoch:           getCurrentEpoch,
		getNetmap:                 getNetmap,
	}
}

func (x *replicationNode) compliesStoragePolicyInPastNetmap(bPubKey []byte, cnrID cid.ID, policy netmapsdk.PlacementPolicy, epoch uint64) (bool, error) {
	nm, err := x.getNetmap(epoch)
	if err != nil {
		return false, fmt.Errorf("read network map: %w", err)
	}

	inNetmap := false
	nodes := nm.Nodes()

	for i := range nodes {
		if bytes.Equal(nodes[i].PublicKey(), bPubKey) {
			inNetmap = true
			break
		}
	}

	if !inNetmap {
		return false, nil
	}

	cnrVectors, err := nm.ContainerNodes(policy, cnrID)
	if err != nil {
		return false, fmt.Errorf("build list of container nodes from network map, storage policy and container ID: %w", err)
	}

	for i := range cnrVectors {
		for j := range cnrVectors[i] {
			if bytes.Equal(cnrVectors[i][j].PublicKey(), bPubKey) {
				return true, nil
			}
		}
	}

	return false, nil
}

// CompliesContainerStoragePolicy checks whether local node's public key is
// presented in network map of the latest NeoFS epoch and matches storage policy
// of the referenced container.
func (x *replicationNode) CompliesContainerStoragePolicy(cnrID cid.ID) (bool, error) {
	storagePolicy, err := x.getContainerStoragePolicy(cnrID)
	if err != nil {
		return false, fmt.Errorf("read container storage policy by container ID: %w", err)
	}

	ok, err := x.compliesStoragePolicyInPastNetmap(x.getPubKey(), cnrID, storagePolicy, x.getCurrentEpoch())
	if err != nil {
		return false, fmt.Errorf("check with the latest network map: %w", err)
	}

	return ok, nil
}

// ClientCompliesContainerStoragePolicy checks whether given public key belongs
// to any storage node present in network map of the latest or previous NeoFS
// epoch and matching storage policy of the referenced container.
func (x *replicationNode) ClientCompliesContainerStoragePolicy(bClientPubKey []byte, cnrID cid.ID) (bool, error) {
	storagePolicy, err := x.getContainerStoragePolicy(cnrID)
	if err != nil {
		return false, fmt.Errorf("read container storage policy by container ID: %w", err)
	}

	curEpoch := x.getCurrentEpoch()

	ok, err := x.compliesStoragePolicyInPastNetmap(bClientPubKey, cnrID, storagePolicy, curEpoch)
	if err != nil {
		return false, fmt.Errorf("check with the latest network map: %w", err)
	}

	if !ok && curEpoch > 0 {
		ok, err = x.compliesStoragePolicyInPastNetmap(bClientPubKey, cnrID, storagePolicy, curEpoch-1)
		if err != nil {
			return false, fmt.Errorf("check with previous network map: %w", err)
		}
	}

	return ok, nil
}

// StoreObject processes object same way as `ObjectService.Put` RPC with TTL=1.
func (x *replicationNode) StoreObject(cnr cid.ID, hdr object.Object, hdrBin []byte, pldBin []byte, pldDataOff int) error {
	return x.putObjectService.ValidateAndStoreObjectLocally(cnr, hdr, hdrBin, pldBin, pldDataOff)
}
