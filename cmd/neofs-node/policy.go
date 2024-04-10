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

// forEachContainerNodePublicKeyInLastTwoEpochs passes binary-encoded public key
// of each node match the referenced container's storage policy at two latest
// epochs into f. When f returns false, nil is returned instantly.
func (x *containerNodes) forEachContainerNodePublicKeyInLastTwoEpochs(cnrID cid.ID, f func(pubKey []byte) bool) error {
	return x.forEachContainerNode(cnrID, true, func(node netmapsdk.NodeInfo) bool {
		return f(node.PublicKey())
	})
}

func (x *containerNodes) forEachContainerNode(cnrID cid.ID, withPrevEpoch bool, f func(netmapsdk.NodeInfo) bool) error {
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

	for i := range ns {
		for j := range ns[i] {
			if !f(ns[i][j]) {
				return nil
			}
		}
	}

	if !withPrevEpoch || epoch == 0 {
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

	for i := range ns {
		for j := range ns[i] {
			if !f(ns[i][j]) {
				return nil
			}
		}
	}

	return nil
}
