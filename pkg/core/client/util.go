package client

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
)

func nodeInfoFromKeyAddr(dst *NodeInfo, k []byte, a network.AddressGroup) {
	dst.SetPublicKey(k)
	dst.SetAddressGroup(a)
}

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

	nodeInfoFromKeyAddr(dst, info.PublicKey(), a)

	return nil
}

// NodeInfoFromNetmapElement fills NodeInfo structure from the interface of parsed netmap member's descriptor.
//
// Args must not be nil.
func NodeInfoFromNetmapElement(dst *NodeInfo, info interface {
	PublicKey() []byte
	Addresses() network.AddressGroup
}) {
	nodeInfoFromKeyAddr(dst, info.PublicKey(), info.Addresses())
}

// AssertKeyResponseCallback returns client response callback which checks if the response was signed by expected key.
// Returns ErrWrongPublicKey in case of key mismatch.
func AssertKeyResponseCallback(expectedKey []byte) func(client.ResponseMetaInfo) error {
	return func(info client.ResponseMetaInfo) error {
		if !bytes.Equal(info.ResponderKey(), expectedKey) {
			return ErrWrongPublicKey
		}

		return nil
	}
}
