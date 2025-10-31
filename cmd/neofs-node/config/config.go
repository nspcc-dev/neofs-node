package config

import (
	"strings"

	apiclientconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/apiclient"
	controlconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/control"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	fschainconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/fschain"
	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	metaconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/meta"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	policerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/policer"
	pprofconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/pprof"
	replicatorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/replicator"
	serviceconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/service"
)

// Config contains all configuration parameters of the node.
type Config struct {
	Logger     loggerconfig.Logger         `mapstructure:"logger"`
	Pprof      pprofconfig.Pprof           `mapstructure:"pprof"`
	Prometheus serviceconfig.Service       `mapstructure:"prometheus"`
	Meta       metaconfig.Meta             `mapstructure:"metadata"`
	Node       nodeconfig.Node             `mapstructure:"node"`
	GRPC       []grpcconfig.GRPC           `mapstructure:"grpc"`
	Control    controlconfig.Control       `mapstructure:"control"`
	FSChain    fschainconfig.Chain         `mapstructure:"fschain"`
	APIClient  apiclientconfig.APIClient   `mapstructure:"apiclient"`
	Policer    policerconfig.Policer       `mapstructure:"policer"`
	Replicator replicatorconfig.Replicator `mapstructure:"replicator"`
	Object     objectconfig.Object         `mapstructure:"object"`
	Storage    engineconfig.Storage        `mapstructure:"storage"`

	isSet map[string]struct{}
	opts  *opts
}

// Normalize sets default values for all fields of Config.
// If some of fields are not set or have invalid values, they will be
// set to default values.
func (c *Config) Normalize() {
	type Normalizable interface {
		Normalize()
	}
	fields := []Normalizable{
		&c.APIClient,
		&c.Storage,
		&c.FSChain,
		&c.Logger,
		&c.Node,
		&c.Object,
		&c.Policer,
		&c.Replicator,
		&c.Pprof,
	}
	for _, field := range fields {
		field.Normalize()
	}
	c.Prometheus.Normalize(serviceconfig.MetricsAddressDefault)
	for i := range c.GRPC {
		c.GRPC[i].Normalize()
	}
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
	clear(c.isSet)
}
