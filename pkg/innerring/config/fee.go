package config

import (
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/spf13/viper"
)

// FeeConfig is an instance that returns extra fee values for contract
// invocations without notary support.
type FeeConfig struct {
	registerNamedCnr,
	mainchain,
	sidechain fixedn.Fixed8
}

// NewFeeConfig constructs FeeConfig from viper.Viper instance. Latter must not be nil.
//
// Fee for named container registration is taken from "fee.named_container_register" value.
func NewFeeConfig(v *viper.Viper) *FeeConfig {
	return &FeeConfig{
		registerNamedCnr: fixedn.Fixed8(v.GetInt64("fee.named_container_register")),
		mainchain:        fixedn.Fixed8(v.GetInt64("fee.main_chain")),
		sidechain:        fixedn.Fixed8(v.GetInt64("fee.side_chain")),
	}
}

func (f FeeConfig) MainChainFee() fixedn.Fixed8 {
	return f.mainchain
}

func (f FeeConfig) SideChainFee() fixedn.Fixed8 {
	return f.sidechain
}

// NamedContainerRegistrationFee returns additional GAS fee for named container registration in NeoFS network.
func (f FeeConfig) NamedContainerRegistrationFee() fixedn.Fixed8 {
	return f.registerNamedCnr
}
