package contractsconfig

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "contracts"
)

// Netmap returns value of "netmap" config parameter
// from "contracts" section.
//
// Returns zero filled script hash if value is not set.
// Throws panic if value is not a 20-byte LE hex-encoded string.
func Netmap(c *config.Config) util.Uint160 {
	return contractAddress(c, "netmap")
}

// Balance returns value of "balance" config parameter
// from "contracts" section.
//
// Returns zero filled script hash if value is not set.
// Throws panic if value is not a 20-byte LE hex-encoded string.
func Balance(c *config.Config) util.Uint160 {
	return contractAddress(c, "balance")
}

// Container returns value of "container" config parameter
// from "contracts" section.
//
// Returns zero filled script hash if value is not set.
// Throws panic if value is not a 20-byte LE hex-encoded string.
func Container(c *config.Config) util.Uint160 {
	return contractAddress(c, "container")
}

// Reputation returns value of "reputation" config parameter
// from "contracts" section.
//
// Returns zero filled script hash if value is not set.
// Throws panic if value is not a 20-byte LE hex-encoded string.
func Reputation(c *config.Config) util.Uint160 {
	return contractAddress(c, "reputation")
}

// Proxy returns value of "proxy" config parameter
// from "contracts" section.
//
// Returns zero filled script hash if value is not set.
// Throws panic if value is not a 20-byte LE hex-encoded string.
func Proxy(c *config.Config) util.Uint160 {
	return contractAddress(c, "proxy")
}

func contractAddress(c *config.Config, name string) util.Uint160 {
	v := config.String(c.Sub(subsection), name)
	if v == "" {
		return util.Uint160{} // if address is not set, then NNS resolver should be used
	}

	addr, err := util.Uint160DecodeStringLE(v)
	if err != nil {
		panic(fmt.Errorf(
			"can't parse %s contract address %s: %w",
			name,
			v,
			err,
		))
	}

	return addr
}
