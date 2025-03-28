package config

import (
	"strings"
	"time"
)

type Config struct {
	Logger Logger `mapstructure:"logger"`

	// NEO Wallet of the node. The wallet is used by Consensus and Notary services.
	//
	// Required.
	Wallet Wallet `mapstructure:"wallet"`

	WithoutMainnet bool `mapstructure:"without_mainnet"`

	Morph Chain `mapstructure:"morph"`

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

	NetmapCleaner NetmapCleaner `mapstructure:"netmap_cleaner"`

	Contracts Contracts `mapstructure:"contracts"`

	Pprof BasicService `mapstructure:"pprof"`

	Prometheus BasicService `mapstructure:"prometheus"`

	Settlement Settlement `mapstructure:"settlement"`

	Experimental Experimental `mapstructure:"experimental"`

	isSet map[string]struct{}
}

type Logger struct {
	Level     string `mapstructure:"level"`
	Encoding  string `mapstructure:"encoding"`
	Timestamp bool   `mapstructure:"timestamp"`
}

type Wallet struct {
	Path     string `mapstructure:"path"`
	Address  string `mapstructure:"address"`
	Password string `mapstructure:"password"`
}

type NNS struct {
	SystemEmail string `mapstructure:"system_email"`
}

type Governance struct {
	Disable bool `mapstructure:"disable"`
}

type Fee struct {
	MainChain int64 `mapstructure:"main_chain"`
}

type Workers struct {
	Alphabet   int `mapstructure:"alphabet"`
	Balance    int `mapstructure:"balance"`
	Container  int `mapstructure:"container"`
	NeoFS      int `mapstructure:"neofs"`
	Netmap     int `mapstructure:"netmap"`
	Reputation int `mapstructure:"reputation"`
}

type Indexer struct {
	CacheTimeout time.Duration `mapstructure:"cache_timeout"`
}

type NetmapCleaner struct {
	Enabled   bool   `mapstructure:"enabled"`
	Threshold uint64 `mapstructure:"threshold"`
}

type BasicService struct {
	Enabled         bool          `mapstructure:"enabled"`
	Address         string        `mapstructure:"address"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

type Settlement struct {
	BasicIncomeRate int64 `mapstructure:"basic_income_rate"`
	AuditFee        int64 `mapstructure:"audit_fee"`
}

type Experimental struct {
	ChainMetaData bool `mapstructure:"chain_meta_data"`
}

// HandleDeprecated normalizes values if outdated names were used.
// Use it after setting all the keys.
func (c *Config) HandleDeprecated() {
	const (
		deprecatedMorphPrefix = "morph"
		fsChainPrefix         = "fschain"
	)
	if (c.IsSet(deprecatedMorphPrefix+".endpoints") || c.IsSet(deprecatedMorphPrefix+".consensus")) &&
		!(c.IsSet(fsChainPrefix+".endpoints") || c.IsSet(fsChainPrefix+".consensus")) {
		c.FSChain = c.Morph
		c.Morph = Chain{}
	}
}

func (c *Config) IsSet(key string) bool {
	_, ok := c.isSet[key]
	return ok
}

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

func (c *Config) Unset(key string) {
	for k := range c.isSet {
		if strings.HasPrefix(k, key) {
			delete(c.isSet, k)
		}
	}
}

func (c *Config) UnsetAll() {
	c.isSet = nil
}
