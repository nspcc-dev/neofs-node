package gas

import (
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativeids"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// DefaultBalance is a balance of every account in redefined [GAS] native
// contract.
const DefaultBalance = 100

var _ = (native.IGAS)(&GAS{})

func (g *GAS) Metadata() *interop.ContractMD {
	return &g.ContractMD
}

// GAS represents GAS custom native contract. It always returns [DefaultBalance] as a
// balance, has no-op `Burn`, `Mint`, `Transfer` operations.
type GAS struct {
	interop.ContractMD
	symbol   string
	decimals int64
	factor   int64
}

// NewGAS returns [GAS] custom native contract.
func NewGAS() *GAS {
	g := &GAS{}
	defer g.BuildHFSpecificMD(g.ActiveIn())

	g.ContractMD = *interop.NewContractMD(nativenames.Gas, nativeids.GasToken, func(m *manifest.Manifest, hf config.Hardfork) {
		m.SupportedStandards = []string{manifest.NEP17StandardName}
	})
	g.symbol = "GAS"
	g.decimals = 8
	g.factor = native.GASFactor

	desc := native.NewDescriptor("symbol", smartcontract.StringType)
	md := native.NewMethodAndPrice(g.Symbol, 0, callflag.NoneFlag)
	g.AddMethod(md, desc)

	desc = native.NewDescriptor("decimals", smartcontract.IntegerType)
	md = native.NewMethodAndPrice(g.Decimals, 0, callflag.NoneFlag)
	g.AddMethod(md, desc)

	desc = native.NewDescriptor("totalSupply", smartcontract.IntegerType)
	md = native.NewMethodAndPrice(g.TotalSupply, 1<<15, callflag.ReadStates)
	g.AddMethod(md, desc)

	desc = native.NewDescriptor("balanceOf", smartcontract.IntegerType,
		manifest.NewParameter("account", smartcontract.Hash160Type))
	md = native.NewMethodAndPrice(g.balanceOf, 1<<15, callflag.ReadStates)
	g.AddMethod(md, desc)

	transferParams := []manifest.Parameter{
		manifest.NewParameter("from", smartcontract.Hash160Type),
		manifest.NewParameter("to", smartcontract.Hash160Type),
		manifest.NewParameter("amount", smartcontract.IntegerType),
	}
	desc = native.NewDescriptor("transfer", smartcontract.BoolType,
		append(transferParams, manifest.NewParameter("data", smartcontract.AnyType))...,
	)
	md = native.NewMethodAndPrice(g.Transfer, 1<<17, callflag.States|callflag.AllowCall|callflag.AllowNotify)
	md.StorageFee = 50
	g.AddMethod(md, desc)

	eDesc := native.NewEventDescriptor("Transfer", transferParams...)
	eMD := native.NewEvent(eDesc)
	g.AddEvent(eMD)

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
	return big.NewInt(DefaultBalance * native.GASFactor)
}

func (g *GAS) Symbol(_ *interop.Context, _ []stackitem.Item) stackitem.Item {
	return stackitem.NewByteArray([]byte(g.symbol))
}

func (g *GAS) Decimals(_ *interop.Context, _ []stackitem.Item) stackitem.Item {
	return stackitem.NewBigInteger(big.NewInt(g.decimals))
}

func (g *GAS) TotalSupply(ic *interop.Context, _ []stackitem.Item) stackitem.Item {
	return stackitem.NewBigInteger(big.NewInt(DefaultBalance * native.GASFactor))
}

func (g *GAS) Transfer(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	return stackitem.NewBool(true)
}

// balanceOf is the only difference with default native GAS implementation:
// it always returns fixed number of tokens.
func (g *GAS) balanceOf(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	return stackitem.NewBigInteger(big.NewInt(DefaultBalance * native.GASFactor))
}

func (g *GAS) Mint(ic *interop.Context, h util.Uint160, amount *big.Int, callOnPayment bool) {
}

func (g *GAS) Burn(ic *interop.Context, h util.Uint160, amount *big.Int) {
}
