package config

// Emit configures settings of GAS emmission.
type Emit struct {
	Storage EmitStorage `mapstructure:"storage"`
	Mint    Mint        `mapstructure:"mint"`
	Gas     Gas         `mapstructure:"gas"`
}

// EmitStorage configures amount of sidechain GAS emitted
// to all storage nodes once per GAS emission cycle.
type EmitStorage struct {
	Amount uint64 `mapstructure:"amount"`
}

// Mint configures settings about received deposit from mainchain.
type Mint struct {
	Value     int64  `mapstructure:"value"`
	CacheSize int    `mapstructure:"cache_size"`
	Threshold uint64 `mapstructure:"threshold"`
}

// Gas configures value of IR wallet balance threshold
// when GAS emission for deposit receivers is disabled.
type Gas struct {
	BalanceThreshold int64 `mapstructure:"balance_threshold"`
}
