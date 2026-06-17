package metaconfig

// Meta contains configuration for Meta service.
type Meta struct {
	Path string `mapstructure:"path"`

	// SeedPort of metadata chain network. Addresses will be used the same as
	// the configured ones in FS chain.
	SeedPort uint16 `mapstructure:"seed_port"`

	// Network addresses to listen Neo metadata P2P on list in the form of
	// "[host]:[port][:announcedPort]".
	P2PAddresses []string `mapstructure:"p2p_addresses"`

	// Neo RPC service configuration.
	//
	// Optional.
	RPCAddresses []string `mapstructure:"rpc_addresses"`
}
