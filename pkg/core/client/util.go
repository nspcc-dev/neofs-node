package client

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
)

func nodeInfoFromKeyAddr(dst *NodeInfo, k []byte, a, external network.AddressGroup) {
	dst.SetPublicKey(k)
	dst.SetAddressGroup(a)
	dst.SetExternalAddressGroup(external)
}

// NodeInfoFromRawNetmapElement fills NodeInfo structure from the interface of raw netmap member's descriptor.
//
// Args must not be nil.
func NodeInfoFromRawNetmapElement(dst *NodeInfo, info interface {
	PublicKey() []byte
	IterateAddresses(func(string) bool)
	NumberOfAddresses() int
	ExternalAddresses() []string
}) error {
	var a network.AddressGroup

	err := a.FromIterator(info)
	if err != nil {
		return fmt.Errorf("parse network address: %w", err)
	}

	var external network.AddressGroup

	ext := info.ExternalAddresses()
	if len(ext) > 0 {
		_ = external.FromStringSlice(ext)
	}

	nodeInfoFromKeyAddr(dst, info.PublicKey(), a, external)

	return nil
}

// NodeInfoFromNetmapElement fills NodeInfo structure from the interface of the parsed netmap member's descriptor.
//
// Args must not be nil.
func NodeInfoFromNetmapElement(dst *NodeInfo, info interface {
	PublicKey() []byte
	Addresses() network.AddressGroup
	ExternalAddresses() network.AddressGroup
}) {
	nodeInfoFromKeyAddr(dst, info.PublicKey(), info.Addresses(), info.ExternalAddresses())
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
