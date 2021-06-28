package nodeconfig

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	utilConfig "github.com/nspcc-dev/neofs-node/pkg/util/config"
)

const (
	subsection = "node"

	attributePrefix = "attribute"
)

var errKeyNotSet = errors.New("empty/not set key address, see `node.key` section")

// Key returns value of "key" config parameter
// from "node" section.
//
// Panics if value is not a non-empty string.
func Key(c *config.Config) *keys.PrivateKey {
	v := config.StringSafe(c.Sub(subsection), "key")
	if v == "" {
		panic(errKeyNotSet)
	}

	var (
		key  *keys.PrivateKey
		err  error
		data []byte
	)
	if data, err = os.ReadFile(v); err == nil {
		key, err = keys.NewPrivateKeyFromBytes(data)
	}

	if err != nil {
		return Wallet(c)
	}

	return key
}

// Wallet returns value of node private key from "node" section.
func Wallet(c *config.Config) *keys.PrivateKey {
	v := c.Sub(subsection).Sub("wallet")
	acc, err := utilConfig.LoadAccount(
		config.String(v, "path"),
		config.String(v, "address"),
		config.String(v, "password"))
	if err != nil {
		panic(fmt.Errorf("invalid wallet config: %w", err))
	}

	return acc.PrivateKey()
}

type stringAddressGroup []string

func (x stringAddressGroup) IterateAddresses(f func(string) bool) {
	for i := range x {
		if f(x[i]) {
			break
		}
	}
}

func (x stringAddressGroup) NumberOfAddresses() int {
	return len(x)
}

// BootstrapAddresses returns value of "addresses" config parameter
// from "node" section as network.AddressGroup.
//
// Panics if value is not a string list of valid NeoFS network addresses.
func BootstrapAddresses(c *config.Config) (addr network.AddressGroup) {
	v := config.StringSlice(c.Sub(subsection), "addresses")

	err := addr.FromIterator(stringAddressGroup(v))
	if err != nil {
		panic(fmt.Errorf("could not parse bootstrap addresses: %w", err))
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
