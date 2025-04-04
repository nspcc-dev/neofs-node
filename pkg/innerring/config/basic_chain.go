package config

import "time"

// BasicChain configures basic settings for every chain.
type BasicChain struct {
	DialTimeout         time.Duration `mapstructure:"dial_timeout"`
	ReconnectionsNumber int           `mapstructure:"reconnections_number"`
	ReconnectionsDelay  time.Duration `mapstructure:"reconnections_delay"`
	Endpoints           []string      `mapstructure:"endpoints"`
}
