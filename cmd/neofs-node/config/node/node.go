package nodeconfig

import (
	"fmt"
	"sort"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	utilConfig "github.com/nspcc-dev/neofs-node/pkg/util/config"
)

const (
	// PersistentStatePathDefault is the default path for persistent state file.
	PersistentStatePathDefault = ".neofs-storage-state"
	// NotificationTimeoutDefault is the default timeout for object notification operation.
	NotificationTimeoutDefault = 5 * time.Second
)

// Node contains configuration for a node.
type Node struct {
	Wallet struct {
		Path     string `mapstructure:"path"`
		Address  string `mapstructure:"address"`
		Password string `mapstructure:"password"`
	} `mapstructure:"wallet"`

	Addresses []string `mapstructure:"addresses"`
	Relay     bool     `mapstructure:"relay"`

	PersistentSessions struct {
		Path string `mapstructure:"path"`
	} `mapstructure:"persistent_sessions"`

	PersistentState struct {
		Path string `mapstructure:"path"`
	} `mapstructure:"persistent_state"`

	Attributes []string `mapstructure:"attributes"`
}

// Normalize sets default values for node fields if they are not set.
func (n *Node) Normalize() {
	if n.PersistentState.Path == "" {
		n.PersistentState.Path = PersistentStatePathDefault
	}
}

// PrivateKey returns the value of a node private key from "node" section.
//
// Panics if section contains invalid values.
func (n *Node) PrivateKey() *keys.PrivateKey {
	acc, err := utilConfig.LoadAccount(n.Wallet.Path, n.Wallet.Address, n.Wallet.Password)
	if err != nil {
		panic(fmt.Errorf("invalid wallet config: %w", err))
	}

	return acc.PrivateKey()
}

// BootstrapAddresses returns the value of "addresses" config parameter
// from "node" section as network.AddressGroup.
//
// Panics if the value is not a string list of valid NeoFS network addresses.
func (n *Node) BootstrapAddresses() (addr network.AddressGroup) {
	err := addr.FromStringSlice(n.Addresses)
	if err != nil {
		panic(fmt.Errorf("could not parse bootstrap addresses: %w", err))
	}
	sort.Sort(addr)
	return addr
}
