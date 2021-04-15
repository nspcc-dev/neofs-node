package config

/*
Config package contains GlobalConfig structure that implements config
reader from both local and global configurations. Most of the time inner ring
does not need this, as for application it has static config with timeouts,
caches sizes etc. However there are routines that use global configuration
values that can be changed in runtime, e.g. basic income rate. Local
configuration value overrides global one so it is easy to debug and test
in different environments.

Implemented as a part of https://github.com/nspcc-dev/neofs-node/issues/363
*/

import (
	netmapClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/spf13/viper"
)

type GlobalConfig struct {
	cfg *viper.Viper
	nm  *netmapClient.Wrapper
}

func NewGlobalConfigReader(cfg *viper.Viper, nm *netmapClient.Wrapper) *GlobalConfig {
	return &GlobalConfig{
		cfg: cfg,
		nm:  nm,
	}
}

func (c *GlobalConfig) BasicIncomeRate() (uint64, error) {
	value := c.cfg.GetUint64("settlement.basic_income_rate")
	if value != 0 {
		return value, nil
	}

	return c.nm.BasicIncomeRate()
}

func (c *GlobalConfig) AuditFee() (uint64, error) {
	value := c.cfg.GetUint64("settlement.audit_fee")
	if value != 0 {
		return value, nil
	}

	return c.nm.AuditFee()
}
