package client

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/network"
)

// NodeInfoFromRawNetmapElement fills NodeInfo structure from the interface of raw netmap member's descriptor.
//
// Args must not be nil.
func NodeInfoFromRawNetmapElement(dst *NodeInfo, info interface {
	PublicKey() []byte
	IterateAddresses(func(string) bool)
	NumberOfAddresses() int
}) error {
	var a network.AddressGroup

	err := a.FromIterator(info)
	if err != nil {
		return fmt.Errorf("parse network address: %w", err)
	}

	dst.SetPublicKey(info.PublicKey())
	dst.SetAddressGroup(a)

	return nil
}
