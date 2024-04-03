package main

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// containerNodes wraps NeoFS network state to apply container storage policies.
type containerNodes struct {
	containers container.Source
	network    netmap.Source
}

func newContainerNodes(containers container.Source, network netmap.Source) (*containerNodes, error) {
	return &containerNodes{
		containers: containers,
		network:    network,
	}, nil
}

// forEachNodePubKeyInSets passes binary-encoded public key of each node into f.
// When f returns false, forEachNodePubKeyInSets returns false instantly.
// Otherwise, true is returned.
func forEachNodePubKeyInSets(nodeSets [][]netmapsdk.NodeInfo, f func(pubKey []byte) bool) bool {
	for i := range nodeSets {
		for j := range nodeSets[i] {
			if !f(nodeSets[i][j].PublicKey()) {
				return false
			}
		}
	}
	return true
}

// forEachContainerNodePublicKeyInLastTwoEpochs passes binary-encoded public key
// of each node match the referenced container's storage policy at two latest
// epochs into f. When f returns false, nil is returned instantly.
func (x *containerNodes) forEachContainerNodePublicKeyInLastTwoEpochs(cnrID cid.ID, f func(pubKey []byte) bool) error {
	epoch, err := x.network.Epoch()
	if err != nil {
		return fmt.Errorf("read current NeoFS epoch: %w", err)
	}

	cnr, err := x.containers.Get(cnrID)
	if err != nil {
		return fmt.Errorf("read container by ID: %w", err)
	}

	networkMap, err := x.network.GetNetMapByEpoch(epoch)
	if err != nil {
		return fmt.Errorf("read network map at epoch #%d: %w", epoch, err)
	}
	// TODO(#2692): node sets remain unchanged for fixed container and network map,
	//  so recently calculated results worth caching
	ns, err := networkMap.ContainerNodes(cnr.Value.PlacementPolicy(), cnrID)
	if err != nil {
		return fmt.Errorf("apply container's storage policy to the network map at epoch #%d: %w", epoch, err)
	}

	if !forEachNodePubKeyInSets(ns, f) || epoch == 0 {
		return nil
	}

	epoch--

	networkMap, err = x.network.GetNetMapByEpoch(epoch)
	if err != nil {
		return fmt.Errorf("read network map at epoch #%d: %w", epoch, err)
	}

	ns, err = networkMap.ContainerNodes(cnr.Value.PlacementPolicy(), cnrID)
	if err != nil {
		return fmt.Errorf("apply container's storage policy to the network map at epoch #%d: %w", epoch, err)
	}

	forEachNodePubKeyInSets(ns, f)

	return nil
}
