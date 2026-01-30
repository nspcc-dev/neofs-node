package gas

import (
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
)

// nep17TokenNative represents a NEP-17 token contract.
type nep17TokenNative struct {
	interop.ContractMD
	symbol   string
	decimals int64
	factor   int64
}

func (c *nep17TokenNative) Metadata() *interop.ContractMD {
	return &c.ContractMD
}

func newNEP17Native(name string, id int32, onManifestConstruction func(m *manifest.Manifest, hf config.Hardfork)) *nep17TokenNative {
	n := &nep17TokenNative{ContractMD: *interop.NewContractMD(name, id, func(m *manifest.Manifest, hf config.Hardfork) {
		m.SupportedStandards = []string{manifest.NEP17StandardName}
		if onManifestConstruction != nil {
			onManifestConstruction(m, hf)
		}
	})}

	desc := native.NewDescriptor("symbol", smartcontract.StringType)
	md := native.NewMethodAndPrice(n.Symbol, 0, callflag.NoneFlag)
	n.AddMethod(md, desc)

	desc = native.NewDescriptor("decimals", smartcontract.IntegerType)
	md = native.NewMethodAndPrice(n.Decimals, 0, callflag.NoneFlag)
	n.AddMethod(md, desc)

	desc = native.NewDescriptor("totalSupply", smartcontract.IntegerType)
	md = native.NewMethodAndPrice(n.TotalSupply, 1<<15, callflag.ReadStates)
	n.AddMethod(md, desc)

	desc = native.NewDescriptor("balanceOf", smartcontract.IntegerType,
		manifest.NewParameter("account", smartcontract.Hash160Type))
	md = native.NewMethodAndPrice(n.balanceOf, 1<<15, callflag.ReadStates)
	n.AddMethod(md, desc)

	transferParams := []manifest.Parameter{
		manifest.NewParameter("from", smartcontract.Hash160Type),
		manifest.NewParameter("to", smartcontract.Hash160Type),
		manifest.NewParameter("amount", smartcontract.IntegerType),
	}
	desc = native.NewDescriptor("transfer", smartcontract.BoolType,
		append(transferParams, manifest.NewParameter("data", smartcontract.AnyType))...,
	)
	md = native.NewMethodAndPrice(n.Transfer, 1<<17, callflag.States|callflag.AllowCall|callflag.AllowNotify)
	md.StorageFee = 50
	n.AddMethod(md, desc)

	eDesc := native.NewEventDescriptor("Transfer", transferParams...)
	eMD := native.NewEvent(eDesc)
	n.AddEvent(eMD)

	return n
}

func (c *nep17TokenNative) Initialize(_ *interop.Context) error {
	return nil
}

func (c *nep17TokenNative) Symbol(_ *interop.Context, _ []stackitem.Item) stackitem.Item {
	return stackitem.NewByteArray([]byte(c.symbol))
}

func (c *nep17TokenNative) Decimals(_ *interop.Context, _ []stackitem.Item) stackitem.Item {
	return stackitem.NewBigInteger(big.NewInt(c.decimals))
}

func (c *nep17TokenNative) TotalSupply(ic *interop.Context, _ []stackitem.Item) stackitem.Item {
	return stackitem.NewBigInteger(big.NewInt(DefaultBalance * native.GASFactor))
}

func (c *nep17TokenNative) Transfer(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	return stackitem.NewBool(true)
}

// balanceOf is the only difference with default native GAS implementation:
// it always returns fixed number of tokens.
func (c *nep17TokenNative) balanceOf(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	return stackitem.NewBigInteger(big.NewInt(DefaultBalance * native.GASFactor))
}

func (c *nep17TokenNative) balanceOfInternal(d *dao.Simple, h util.Uint160) *big.Int {
	return big.NewInt(DefaultBalance * native.GASFactor)
}

func (c *nep17TokenNative) Mint(ic *interop.Context, h util.Uint160, amount *big.Int, callOnPayment bool) {
}

func (c *nep17TokenNative) Burn(ic *interop.Context, h util.Uint160, amount *big.Int) {
}
