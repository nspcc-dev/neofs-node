package config

import (
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
)

// Chain configures settings of FS chain.
type Chain struct {
	BasicChain `mapstructure:",squash"`
	Validators keys.PublicKeys `mapstructure:"validators"`
	Consensus  Consensus       `mapstructure:"consensus"`
}

// Consensus configures Blockchain. All required fields must be set. Specified
// optional fields tune Blockchain's default behavior (zero or omitted values).
//
// See docs of NeoGo configuration for some details.
type Consensus struct {
	// Identifier of the Neo network.
	//
	// Required.
	Magic uint32 `mapstructure:"magic"`

	// Initial committee staff.
	//
	// Required.
	Committee keys.PublicKeys `mapstructure:"committee"`

	// Storage configuration. Must be set using one of constructors like BoltDB.
	//
	// Required.
	Storage Storage `mapstructure:"storage"`

	// Time period (approximate) between two adjacent blocks.
	//
	// Optional: defaults to 15s. Must not be negative.
	TimePerBlock time.Duration `mapstructure:"time_per_block"`

	// Length of the chain accessible to smart contracts.
	//
	// Optional: defaults to 17280.
	MaxTraceableBlocks uint32 `mapstructure:"max_traceable_blocks"`

	// List of nodes' addresses to communicate with over Neo P2P protocol in
	// 'host:port' format.
	//
	// Optional: by default, node runs as standalone.
	SeedNodes []string `mapstructure:"seed_nodes"`

	// Maps hard-fork's name to the appearance chain height.
	//
	// Optional: by default, each known hard-fork is applied from the zero
	// blockchain height.
	Hardforks Hardforks `mapstructure:"hardforks"`

	// Maps chain height to number of consensus nodes.
	//
	// Optional: by default Committee size is used. Each value must be positive and
	// must not exceed Committee length. Value for zero key (genesis height) is
	// required.
	ValidatorsHistory ValidatorsHistory `mapstructure:"validators_history"`

	// Neo RPC service configuration.
	//
	// Optional: see RPC defaults.
	RPC RPC `mapstructure:"rpc"`

	// P2P settings.
	//
	// Required.
	P2P P2P `mapstructure:"p2p"`

	// Whether to designate [noderoles.P2PNotary] and [noderoles.NeoFSAlphabet]
	// roles to the Committee (keep an eye on ValidatorsHistory) for genesis block
	// in the RoleManagement contract.
	//
	// Optional: by default, roles are unset.
	SetRolesInGenesis bool `mapstructure:"set_roles_in_genesis"`

	// KeepOnlyLatestState specifies if MPT should only store the latest state.
	// If true, DB size will be smaller, but older roots won't be accessible.
	// This value should remain the same for the same database.
	//
	// Optional: by default, false.
	KeepOnlyLatestState bool `mapstructure:"keep_only_latest_state"`

	// RemoveUntraceableBlocks specifies if old data should be removed.
	//
	// Optional: by default, false.
	RemoveUntraceableBlocks bool `mapstructure:"remove_untraceable_blocks"`

	// Memory pool size for P2PNotaryRequestPayloads.
	//
	// Optional: defaults to 1000.
	P2PNotaryRequestPayloadPoolSize uint32 `mapstructure:"p2p_notary_request_payload_pool_size"`
}

// Storage configures Blockchain storage.
type Storage struct {
	Type string `mapstructure:"type"`
	Path string `mapstructure:"path"`
}

// Hardforks configures the matching of hard-fork's name to the appearance chain height.
type Hardforks struct {
	Name map[string]uint32 `mapstructure:",remain"`
}

// ValidatorsHistory configures the matching of chain height to number of consensus nodes.
type ValidatorsHistory struct {
	Height map[uint32]uint32 `mapstructure:",remain"`
}

// RPC configures RPC serving.
type RPC struct {
	// Network addresses to listen Neo RPC on. Each element must be a valid TCP
	// address in 'host:port' format.
	//
	// Optional: by default, insecure Neo RPC is not served.
	Listen []string `mapstructure:"listen"`

	// The maximum simultaneous websocket client connection number.
	//
	// Optional: defaults to 64. Must not be larger than math.MaxInt32.
	MaxWebSocketClients uint32 `mapstructure:"max_websocket_clients"`

	// The maximum number of concurrent iterator sessions.
	//
	// Optional: defaults to 20. Must not be larger than math.MaxInt32.
	SessionPoolSize uint32 `mapstructure:"session_pool_size"`

	// The maximum amount of GAS which can be spent during an RPC call.
	//
	// Optional: defaults to 100. Must not be larger than math.MaxInt32.
	MaxGasInvoke uint32 `mapstructure:"max_gas_invoke"`

	// Additional addresses that use TLS.
	//
	// Optional.
	TLS TLS `mapstructure:"tls"`
}

// TLS configures additional RPC serving over TLS.
type TLS struct {
	// Additional TLS serving switcher.
	//
	// Optional: by default TLS is switched off.
	Enabled bool `mapstructure:"enabled"`

	// Network addresses to listen Neo RPC on if Enabled. Each element must be a valid TCP
	// address in 'host:port' format.
	Listen []string `mapstructure:"listen"`

	// TLS certificate file path.
	//
	// Required if Enabled and one or more addresses are provided.
	CertFile string `mapstructure:"cert_file"`

	// TLS private key file path.
	//
	// Required if Enabled and one or more addresses are provided.
	KeyFile string `mapstructure:"key_file"`
}

// P2P configures communication over Neo P2P protocol.
type P2P struct {
	// Maximum duration a single dial may take.
	//
	// Optional: defaults to 1m. Must not be negative.
	DialTimeout time.Duration `mapstructure:"dial_timeout"`

	// Interval between protocol ticks with each connected peer.
	//
	// Optional: defaults to 2s. Must not be negative.
	ProtoTickInterval time.Duration `mapstructure:"proto_tick_interval"`

	// Network addresses to listen Neo P2P on. Each element must be a valid TCP
	// address in 'host:port' format.
	//
	// Optional: by default, Neo P2P is not served.
	Listen []string `mapstructure:"listen"`

	Peers Peers `mapstructure:"peers"`

	// Pinging mechanism.
	//
	// Optional: see P2P defaults.
	Ping Ping `mapstructure:"ping"`
}

// Peers configures peers.
type Peers struct {
	// Specifies the minimum number of peers a node needs for normal operation.
	//
	// Required. Must not be larger than math.MaxInt32.
	Min uint32 `mapstructure:"min"`

	// Limits maximum number of peers dealing with the node.
	//
	// Optional: defaults to 100. Must not be larger than math.MaxInt32.
	Max uint32 `mapstructure:"max"`

	// Specifies how many peers node should try to dial when connection counter
	// drops below the Min value.
	//
	// Optional: defaults to Min+10. Must not be greater than math.MaxInt32.
	Attempts uint32 `mapstructure:"attempts"`
}

// Ping configures P2P pinging mechanism.
type Ping struct {
	// Interval between pings.
	//
	// Optional: defaults to 30s. Must not be negative.
	Interval time.Duration `mapstructure:"interval"`

	// Time period to wait for pong.
	//
	// Optional: defaults to 1m. Must not be negative.
	Timeout time.Duration `mapstructure:"timeout"`
}
