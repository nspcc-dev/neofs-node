package config

type Emit struct {
	Storage EmitStorage `mapstructure:"storage"`
	Mint    Mint        `mapstructure:"mint"`
	Gas     Gas         `mapstructure:"gas"`
}

type EmitStorage struct {
	Amount uint64 `mapstructure:"amount"`
}

type Mint struct {
	Value     int64  `mapstructure:"value"`
	CacheSize int    `mapstructure:"cache_size"`
	Threshold uint64 `mapstructure:"threshold"`
}

type Gas struct {
	BalanceThreshold int64 `mapstructure:"balance_threshold"`
}
