package client

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
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

// AssertKeyResponseCallback returns client response callback which checks if the response was signed by the expected key.
// Returns ErrWrongPublicKey in case of key mismatch.
func AssertKeyResponseCallback(expectedKey []byte) func(client.ResponseMetaInfo) error {
	return func(info client.ResponseMetaInfo) error {
		if !bytes.Equal(info.ResponderKey(), expectedKey) {
			return ErrWrongPublicKey
		}

		return nil
	}
}
