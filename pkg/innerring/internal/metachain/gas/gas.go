package gas

import (
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativeids"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// DefaultBalance is a balance of every account in redefined [GAS] native
// contract.
const DefaultBalance = 100

var _ = (native.IGAS)(&GAS{})

// GAS represents GAS custom native contract. It always returns [DefaultBalance] as a
// balance, has no-op `Burn`, `Mint`, `Transfer` operations.
type GAS struct {
	nep17TokenNative
}

// NewGAS returns [GAS] custom native contract.
func NewGAS() *GAS {
	g := &GAS{}
	defer g.BuildHFSpecificMD(g.ActiveIn())

	nep17 := newNEP17Native(nativenames.Gas, nativeids.GasToken, nil)
	nep17.symbol = "GAS"
	nep17.decimals = 8
	nep17.factor = native.GASFactor

	g.nep17TokenNative = *nep17

	return g
}

// Initialize initializes a GAS contract.
func (g *GAS) Initialize(ic *interop.Context, hf *config.Hardfork, newMD *interop.HFSpecificContractMD) error {
	return nil
}

// InitializeCache implements the [interop.Contract] interface.
func (g *GAS) InitializeCache(_ interop.IsHardforkEnabled, blockHeight uint32, d *dao.Simple) error {
	return nil
}

// OnPersist implements the [interop.Contract] interface.
func (g *GAS) OnPersist(ic *interop.Context) error {
	return nil
}

// PostPersist implements the [interop.Contract] interface.
func (g *GAS) PostPersist(ic *interop.Context) error {
	return nil
}

// ActiveIn implements the [interop.Contract] interface.
func (g *GAS) ActiveIn() *config.Hardfork {
	return nil
}

// BalanceOf returns native GAS token balance for the acc.
func (g *GAS) BalanceOf(d *dao.Simple, acc util.Uint160) *big.Int {
	return g.balanceOfInternal(d, acc)
}
