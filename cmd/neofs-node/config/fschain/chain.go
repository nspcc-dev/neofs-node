package fschainconfig

import (
	"time"
)

const (
	// DialTimeoutDefault is the default dial timeout of FS chain client connection.
	DialTimeoutDefault = time.Minute
	// CacheTTLDefault is the default value for cached values TTL.
	CacheTTLDefault = 15 * time.Second
	// QuotaTTLDefault is the default value for FS chain quotas TTL.
	QuotaTTLDefault = 10 * time.Minute
	// ReconnectionRetriesNumberDefault is the default value for reconnection retries.
	ReconnectionRetriesNumberDefault = 5
	// ReconnectionRetriesDelayDefault is the default delay b/w reconnections.
	ReconnectionRetriesDelayDefault = 5 * time.Second
)

// Chain contains configuration for NeoFS client connection to FS chain.
type Chain struct {
	DialTimeout         time.Duration `mapstructure:"dial_timeout"`
	CacheTTL            time.Duration `mapstructure:"cache_ttl"`
	QuotaCacheTTL       time.Duration `mapstructure:"quota_ttl"`
	ReconnectionsNumber int           `mapstructure:"reconnections_number"`
	ReconnectionsDelay  time.Duration `mapstructure:"reconnections_delay"`
	Endpoints           []string      `mapstructure:"endpoints"`
}

// Normalize ensures that all fields of Chain have valid values.
// If some of fields are not set or have invalid values, they will be
// set to default values.
func (c *Chain) Normalize() {
	if c.DialTimeout <= 0 {
		c.DialTimeout = DialTimeoutDefault
	}
	if c.CacheTTL == 0 {
		c.CacheTTL = CacheTTLDefault
	}
	if c.QuotaCacheTTL == 0 {
		c.QuotaCacheTTL = QuotaTTLDefault
	}
	if c.ReconnectionsNumber == 0 {
		c.ReconnectionsNumber = ReconnectionRetriesNumberDefault
	}
	if c.ReconnectionsDelay == 0 {
		c.ReconnectionsDelay = ReconnectionRetriesDelayDefault
	}
}
