package config

import (
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/spf13/viper"
)

// FeeConfig is an instance that returns extra fee values for contract
// invocations without notary support.
type FeeConfig struct {
	mainchain fixedn.Fixed8
}

// NewFeeConfig constructs FeeConfig from viper.Viper instance. Latter must not be nil.
func NewFeeConfig(v *viper.Viper) *FeeConfig {
	return &FeeConfig{
		mainchain: fixedn.Fixed8(v.GetInt64("fee.main_chain")),
	}
}

func (f FeeConfig) MainChainFee() fixedn.Fixed8 {
	return f.mainchain
}
