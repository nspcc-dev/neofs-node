package config

import "time"

// TODO
type MetaChain struct {
	// Swith experimental meta-on-chain support.
	Enabled bool `mapstructure:"enabled"`

	// List of nodes' addresses to communicate with over Neo P2P protocol in
	// 'host:port' format.
	//
	// Optional: by default, node runs as standalone.
	SeedNodes []string `mapstructure:"seed_nodes"`

	// Storage configuration. Must be set using one of constructors like BoltDB.
	//
	// Required.
	Storage Storage `mapstructure:"storage"`

	// Maximum time period (approximate) between two adjacent blocks,
	// if used enables dynamic block time (contrary to TimePerBlock
	// targeting for every block).
	//
	// Optional: not set by default. Must not be negative, must be
	// bigger than TimePerBlock.
	MaxTimePerBlock time.Duration `mapstructure:"max_time_per_block"`

	// Neo RPC service configuration.
	//
	// Optional: see RPC defaults.
	RPC RPC `mapstructure:"rpc"`

	// P2P settings.
	//
	// Required.
	P2P P2P `mapstructure:"p2p"`
}
