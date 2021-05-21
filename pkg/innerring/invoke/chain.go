package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
)

type (
	// SideFeeProvider is an interface that used by invoker package method to
	// get extra fee for all non notary smart contract invocations in side chain.
	SideFeeProvider interface {
		SideChainFee() fixedn.Fixed8
	}

	// MainFeeProvider is an interface that used by invoker package method to
	// get extra fee for all non notary smart contract invocations in main chain.
	MainFeeProvider interface {
		MainChainFee() fixedn.Fixed8
	}
)
