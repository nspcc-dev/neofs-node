package config

import "time"

type Audit struct {
	Timeout Timeout `mapstructure:"timeout"`
	Task    Task    `mapstructure:"task"`
	PDP     PDP     `mapstructure:"pdp"`
	POR     POR     `mapstructure:"por"`
}

type Timeout struct {
	Get       time.Duration `mapstructure:"get"`
	Head      time.Duration `mapstructure:"head"`
	RangeHash time.Duration `mapstructure:"rangehash"`
	Search    time.Duration `mapstructure:"search"`
}

type Task struct {
	ExecPoolSize  int    `mapstructure:"exec_pool_size"`
	QueueCapacity uint32 `mapstructure:"queue_capacity"`
}

type PDP struct {
	PairsPoolSize    int           `mapstructure:"pairs_pool_size"`
	MaxSleepInterval time.Duration `mapstructure:"max_sleep_interval"`
}

type POR struct {
	PoolSize int `mapstructure:"pool_size"`
}
