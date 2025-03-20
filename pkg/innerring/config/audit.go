package config

import "time"

// Audit configures settings for data audit.
type Audit struct {
	Timeout Timeout `mapstructure:"timeout"`
	Task    Task    `mapstructure:"task"`
	PDP     PDP     `mapstructure:"pdp"`
	POR     POR     `mapstructure:"por"`
}

// Timeout configures timeouts for operations during data audit.
type Timeout struct {
	Get       time.Duration `mapstructure:"get"`
	Head      time.Duration `mapstructure:"head"`
	RangeHash time.Duration `mapstructure:"rangehash"`
	Search    time.Duration `mapstructure:"search"`
}

// Task configures settings for parallel audit jobs.
type Task struct {
	ExecPoolSize  int    `mapstructure:"exec_pool_size"`
	QueueCapacity uint32 `mapstructure:"queue_capacity"`
}

// PDP configures settings for PDP part of audit.
type PDP struct {
	PairsPoolSize    int           `mapstructure:"pairs_pool_size"`
	MaxSleepInterval time.Duration `mapstructure:"max_sleep_interval"`
}

// POR configures settings for Por part of audit.
type POR struct {
	PoolSize int `mapstructure:"pool_size"`
}
