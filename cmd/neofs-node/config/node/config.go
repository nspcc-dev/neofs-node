package nodeconfig

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/network"
)

const (
	subsection = "node"

	attributePrefix = "attribute"
)

var (
	errKeyNotSet     = errors.New("empty/not set key address, see `node.key` section")
	errAddressNotSet = errors.New("empty/not set bootstrap address, see `node.address` section")
)

// Key returns value of "key" config parameter
// from "node" section.
//
// Panics if value is not a non-empty string.
func Key(c *config.Config) string {
	v := config.StringSafe(c.Sub(subsection), "key")
	if v == "" {
		panic(errKeyNotSet)
	}

	// TODO: add string -> `ecdsa.PrivateKey` parsing logic
	// after https://github.com/nspcc-dev/neofs-node/pull/569.

	return v
}

// BootstrapAddress returns value of "address" config parameter
// from "node" section as network.Address.
//
// Panics if value is not a valid NeoFS network address
func BootstrapAddress(c *config.Config) *network.Address {
	v := config.StringSafe(c.Sub(subsection), "address")
	if v == "" {
		panic(errAddressNotSet)
	}

	addr, err := network.AddressFromString(v)
	if err != nil {
		panic(fmt.Errorf("could not convert bootstrap address %s to %T: %w", v, addr, err))
	}

	return addr
}

// Attributes returns list of config parameters
// from "node" section that are set in "attribute_i" format,
// where i in range [0,100).
func Attributes(c *config.Config) (attrs []string) {
	const maxAttributes = 100

	for i := 0; i < maxAttributes; i++ {
		attr := config.StringSafe(c.Sub(subsection), attributePrefix+"_"+strconv.Itoa(i))
		if attr == "" {
			return
		}

		attrs = append(attrs, attr)
	}

	return
}

// Relay returns value of "relay" config parameter
// from "node" section.
//
// Returns false if value is not set.
func Relay(c *config.Config) bool {
	return config.BoolSafe(c.Sub(subsection), "relay")
}
