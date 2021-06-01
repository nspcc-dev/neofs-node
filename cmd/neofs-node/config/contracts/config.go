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
// Throws panic if value is not is not a 20-byte LE hex-encoded string.
func Netmap(c *config.Config) util.Uint160 {
	return contractAddress(c, "netmap")
}

// Balance returns value of "balance" config parameter
// from "contracts" section.
//
// Throws panic if value is not is not a 20-byte LE hex-encoded string.
func Balance(c *config.Config) util.Uint160 {
	return contractAddress(c, "balance")
}

// Container returns value of "container" config parameter
// from "contracts" section.
//
// Throws panic if value is not is not a 20-byte LE hex-encoded string.
func Container(c *config.Config) util.Uint160 {
	return contractAddress(c, "container")
}

// Reputation returns value of "reputation" config parameter
// from "contracts" section.
//
// Throws panic if value is not is not a 20-byte LE hex-encoded string.
func Reputation(c *config.Config) util.Uint160 {
	return contractAddress(c, "reputation")
}

func contractAddress(c *config.Config, name string) util.Uint160 {
	v := config.String(c.Sub(subsection), name)
	if v == "" {
		panic(fmt.Errorf(
			"empty %s contract address, see `contracts.%s` section",
			name,
			name,
		))
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
