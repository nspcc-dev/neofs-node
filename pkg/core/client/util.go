package client

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// NodeInfoFromRawNetmapElement fills NodeInfo structure from the interface of raw netmap member's descriptor.
//
// Args must not be nil.
func NodeInfoFromRawNetmapElement(dst *NodeInfo, ni netmap.NodeInfo) error {
	var a network.AddressGroup

	err := a.FromNodeInfo(ni)
	if err != nil {
		return fmt.Errorf("parse network address: %w", err)
	}

	dst.SetPublicKey(ni.PublicKey())
	dst.SetAddressGroup(a)

	return nil
}
