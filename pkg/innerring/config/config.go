package config

import (
	"strings"
	"time"
)

// Config configures IR node.
type Config struct {
	Logger Logger `mapstructure:"logger"`

	// NEO Wallet of the node. The wallet is used by Consensus and Notary services.
	//
	// Required.
	Wallet Wallet `mapstructure:"wallet"`

	WithoutMainnet bool `mapstructure:"without_mainnet"`

	FSChain Chain `mapstructure:"fschain"`

	FSChainAutodeploy bool `mapstructure:"fschain_autodeploy"`

	NNS NNS `mapstructure:"nns"`

	Mainnet BasicChain `mapstructure:"mainnet"`

	Control Control `mapstructure:"control"`

	Governance Governance `mapstructure:"governance"`

	Node Node `mapstructure:"node"`

	// Fee is an instance that returns extra fee values for contract
	// invocations without notary support.
	Fee Fee `mapstructure:"fee"`

	Timers Timers `mapstructure:"timers"`

	Emit Emit `mapstructure:"emit"`

	Workers Workers `mapstructure:"workers"`

	Audit Audit `mapstructure:"audit"`

	Indexer Indexer `mapstructure:"indexer"`

	Contracts Contracts `mapstructure:"contracts"`

	Pprof BasicService `mapstructure:"pprof"`

	Prometheus BasicService `mapstructure:"prometheus"`

	Validator Validator `mapstructure:"sn_validator"`

	Settlement Settlement `mapstructure:"settlement"`

	Experimental Experimental `mapstructure:"experimental"`

	isSet map[string]struct{}
}

// Sampling configures log sampling.
type Sampling struct {
	Enabled bool `mapstructure:"enabled"`
}

// Logger configures logger settings.
type Logger struct {
	Level     string   `mapstructure:"level"`
	Encoding  string   `mapstructure:"encoding"`
	Timestamp bool     `mapstructure:"timestamp"`
	Sampling  Sampling `mapstructure:"sampling"`
}

// Wallet configures NEO wallet settings.
type Wallet struct {
	Path     string `mapstructure:"path"`
	Address  string `mapstructure:"address"`
	Password string `mapstructure:"password"`
}

// NNS configures NNS domains processed during the FS chain deployment.
type NNS struct {
	SystemEmail string `mapstructure:"system_email"`
}

// Governance configures synchronization of sidechain committee and mainchain role management contract.
type Governance struct {
	Disable bool `mapstructure:"disable"`
}

// Fee configures extra GAS fee for mainchain contract invocation.
type Fee struct {
	MainChain int64 `mapstructure:"main_chain"`
}

// Workers configures number of workers to process events from contracts in parallel.
type Workers struct {
	Alphabet   int `mapstructure:"alphabet"`
	Balance    int `mapstructure:"balance"`
	Container  int `mapstructure:"container"`
	NeoFS      int `mapstructure:"neofs"`
	Netmap     int `mapstructure:"netmap"`
	Reputation int `mapstructure:"reputation"`
}

// Indexer configures duration between internal state update about current list of inner ring nodes.
type Indexer struct {
	CacheTimeout time.Duration `mapstructure:"cache_timeout"`
}

// BasicService configures settings of basic external service like pprof or prometheus.
type BasicService struct {
	Enabled         bool          `mapstructure:"enabled"`
	Address         string        `mapstructure:"address"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// Validator represents the configuration for an external validation service.
type Validator struct {
	Enabled bool   `mapstructure:"enabled"`
	URL     string `mapstructure:"url"`
}

// Settlement overrides some settlements from network config.
// Applied only in debug mode.
type Settlement struct {
	BasicIncomeRate int64 `mapstructure:"basic_income_rate"`
	AuditFee        int64 `mapstructure:"audit_fee"`
}

// Experimental configures experimental features.
type Experimental struct {
	ChainMetaData bool `mapstructure:"chain_meta_data"`
}

// IsSet checks if the key is set in the config.
func (c *Config) IsSet(key string) bool {
	_, ok := c.isSet[key]
	return ok
}

// Set specifies that the key has been set in the config.
func (c *Config) Set(key string) {
	if c.isSet == nil {
		c.isSet = make(map[string]struct{})
	}
	keySplit := strings.Split(key, ".")
	s := keySplit[0]
	for i := 1; i < len(keySplit); i++ {
		c.isSet[s] = struct{}{}
		s += "." + keySplit[i]
	}
	c.isSet[key] = struct{}{}
}

// Unset ensures that the key is unset in the config.
func (c *Config) Unset(key string) {
	for k := range c.isSet {
		if strings.HasPrefix(k, key) {
			delete(c.isSet, k)
		}
	}
}

// UnsetAll unsets all keys from config.
func (c *Config) UnsetAll() {
	c.isSet = nil
}
