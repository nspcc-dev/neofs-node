package nodeconfig

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	utilConfig "github.com/nspcc-dev/neofs-node/pkg/util/config"
)

// PersistentSessionsConfig is a wrapper over "persistent_sessions" config section
// which provides access to persistent session tokens storage configuration of node.
type PersistentSessionsConfig struct {
	cfg *config.Config
}

// PersistentStateConfig is a wrapper over "persistent_state" config section
// which provides access to persistent state storage configuration of node.
type PersistentStateConfig struct {
	cfg *config.Config
}

const (
	subsection                   = "node"
	persistentSessionsSubsection = "persistent_sessions"
	persistentStateSubsection    = "persistent_state"

	attributePrefix = "attribute"

	// PersistentStatePathDefault is a default path for persistent state file.
	PersistentStatePathDefault = ".neofs-storage-state"

	// NotificationTimeoutDefault is a default timeout for object notification operation.
	NotificationTimeoutDefault = 5 * time.Second
)

// Wallet returns the value of a node private key from "node" section.
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
		if !f(x[i]) {
			break
		}
	}
}

func (x stringAddressGroup) NumberOfAddresses() int {
	return len(x)
}

// BootstrapAddresses returns the value of "addresses" config parameter
// from "node" section as network.AddressGroup.
//
// Panics if the value is not a string list of valid NeoFS network addresses.
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

	for i := range maxAttributes {
		attr := config.StringSafe(c.Sub(subsection), attributePrefix+"_"+strconv.Itoa(i))
		if attr == "" {
			return
		}

		attrs = append(attrs, attr)
	}

	return
}

// Relay returns the value of "relay" config parameter
// from "node" section.
//
// Returns false if the value is not set.
func Relay(c *config.Config) bool {
	return config.BoolSafe(c.Sub(subsection), "relay")
}

// PersistentSessions returns structure that provides access to "persistent_sessions"
// subsection of "node" section.
func PersistentSessions(c *config.Config) PersistentSessionsConfig {
	return PersistentSessionsConfig{
		c.Sub(subsection).Sub(persistentSessionsSubsection),
	}
}

// Path returns the value of "path" config parameter.
func (p PersistentSessionsConfig) Path() string {
	return config.String(p.cfg, "path")
}

// PersistentState returns structure that provides access to "persistent_state"
// subsection of "node" section.
func PersistentState(c *config.Config) PersistentStateConfig {
	return PersistentStateConfig{
		c.Sub(subsection).Sub(persistentStateSubsection),
	}
}

// Path returns the value of "path" config parameter.
//
// Returns PersistentStatePathDefault if the value is not a non-empty string.
func (p PersistentStateConfig) Path() string {
	v := config.String(p.cfg, "path")
	if v != "" {
		return v
	}

	return PersistentStatePathDefault
}
