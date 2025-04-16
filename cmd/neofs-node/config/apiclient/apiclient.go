package apiclientconfig

import (
	"time"
)

const (
	// StreamTimeoutDefault is the default timeout of NeoFS API streaming operation.
	StreamTimeoutDefault = time.Minute
	// MinConnTimeDefault is the default minimum time of established connection in NeoFS API client.
	MinConnTimeDefault = 20 * time.Second
	// PingIntervalDefault is the default interval between pings in NeoFS API client.
	PingIntervalDefault = 10 * time.Second
	// PingTimeoutDefault is the default timeout of a single ping in NeoFS API client.
	PingTimeoutDefault = 5 * time.Second
)

// APIClient contains configuration for NeoFS API client.
type APIClient struct {
	StreamTimeout     time.Duration `mapstructure:"stream_timeout"`
	MinConnectionTime time.Duration `mapstructure:"min_connection_time"`
	PingInterval      time.Duration `mapstructure:"ping_interval"`
	PingTimeout       time.Duration `mapstructure:"ping_timeout"`
}

// Normalize ensures that all fields of APIClient have valid values.
// If some of fields are not set or have invalid values, they will be
// set to default values.
func (c *APIClient) Normalize() {
	if c.StreamTimeout <= 0 {
		c.StreamTimeout = StreamTimeoutDefault
	}
	if c.MinConnectionTime <= 0 {
		c.MinConnectionTime = MinConnTimeDefault
	}
	if c.PingInterval <= 0 {
		c.PingInterval = PingIntervalDefault
	}
	if c.PingTimeout <= 0 {
		c.PingTimeout = PingTimeoutDefault
	}
}
