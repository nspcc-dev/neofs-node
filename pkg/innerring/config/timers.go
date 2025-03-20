package config

type Timers struct {
	StopEstimation        BasicTimer `mapstructure:"stop_estimation"`
	CollectBasicIncome    BasicTimer `mapstructure:"collect_basic_income"`
	DistributeBasicIncome BasicTimer `mapstructure:"distribute_basic_income"`
}

type BasicTimer struct {
	Mul uint32 `mapstructure:"mul"`
	Div uint32 `mapstructure:"div"`
}
