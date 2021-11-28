package nodeconfig

import (
	"fmt"
	"os"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	utilConfig "github.com/nspcc-dev/neofs-node/pkg/util/config"
)

// PersistentStateConfig is a wrapper over "persistent_state" config section
// which provides access to persistent state storage configuration of node.
type PersistentStateConfig struct {
	cfg *config.Config
}

const (
	subsection                = "node"
	persistentStateSubsection = "persistent_state"

	attributePrefix = "attribute"

	// PersistentStatePathDefault is a default path for persistent state file.
	PersistentStatePathDefault = ".neofs-storage-state"
)

// Key returns value of "key" config parameter
// from "node" section.
//
// If value is not set, fallbacks to Wallet section.
//
// Panics if value is incorrect filename of binary encoded private key.
func Key(c *config.Config) *keys.PrivateKey {
	v := config.StringSafe(c.Sub(subsection), "key")
	if v == "" {
		return Wallet(c)
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
		panic(fmt.Errorf("invalid private key in node section: %w", err))
	}

	return key
}

// Wallet returns value of node private key from "node" section.
//
// Panics if section contains invalid values.
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

// PersistentState returns structure that provides access to "persistent_state"
// subsection of "node" section.
func PersistentState(c *config.Config) PersistentStateConfig {
	return PersistentStateConfig{
		c.Sub(subsection).Sub(persistentStateSubsection),
	}
}

// Path returns value of "path" config parameter.
//
// Returns PersistentStatePathDefault if value is not a non-empty string.
func (p PersistentStateConfig) Path() string {
	v := config.String(p.cfg, "path")
	if v != "" {
		return v
	}

	return PersistentStatePathDefault
}

// SubnetConfig represents node configuration related to subnets.
type SubnetConfig config.Config

// Init initializes SubnetConfig from "subnet" sub-section of "node" section
// of the root config.
func (x *SubnetConfig) Init(root config.Config) {
	*x = SubnetConfig(*root.Sub(subsection).Sub("subnet"))
}

// ExitZero returns value of "exit_zero" config parameter as bool.
// Returns false if value can not be cast.
func (x SubnetConfig) ExitZero() bool {
	return config.BoolSafe((*config.Config)(&x), "exit_zero")
}

// IterateSubnets casts value of "entries" config parameter to string slice,
// iterates over all of its elements and passes them to f.
//
// Does nothing if value can not be cast to string slice.
func (x SubnetConfig) IterateSubnets(f func(string)) {
	ids := config.StringSliceSafe((*config.Config)(&x), "entries")

	for i := range ids {
		f(ids[i])
	}
}
