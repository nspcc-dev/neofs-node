package config

// Timers configures timers for operations within the epoch.
type Timers struct {
	CollectBasicIncome    BasicTimer `mapstructure:"collect_basic_income"`
	DistributeBasicIncome BasicTimer `mapstructure:"distribute_basic_income"`
}

// BasicTimer configures basic settings for all Timers.
type BasicTimer struct {
	Mul uint32 `mapstructure:"mul"`
	Div uint32 `mapstructure:"div"`
}
