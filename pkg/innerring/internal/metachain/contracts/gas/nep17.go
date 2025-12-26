package gas

import (
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/encoding/bigint"
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

// totalSupplyKey is the key used to store totalSupply value.
var totalSupplyKey = []byte{11}

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

	desc := NewDescriptor("symbol", smartcontract.StringType)
	md := NewMethodAndPrice(n.Symbol, 0, callflag.NoneFlag)
	n.AddMethod(md, desc)

	desc = NewDescriptor("decimals", smartcontract.IntegerType)
	md = NewMethodAndPrice(n.Decimals, 0, callflag.NoneFlag)
	n.AddMethod(md, desc)

	desc = NewDescriptor("totalSupply", smartcontract.IntegerType)
	md = NewMethodAndPrice(n.TotalSupply, 1<<15, callflag.ReadStates)
	n.AddMethod(md, desc)

	desc = NewDescriptor("balanceOf", smartcontract.IntegerType,
		manifest.NewParameter("account", smartcontract.Hash160Type))
	md = NewMethodAndPrice(n.balanceOf, 1<<15, callflag.ReadStates)
	n.AddMethod(md, desc)

	transferParams := []manifest.Parameter{
		manifest.NewParameter("from", smartcontract.Hash160Type),
		manifest.NewParameter("to", smartcontract.Hash160Type),
		manifest.NewParameter("amount", smartcontract.IntegerType),
	}
	desc = NewDescriptor("transfer", smartcontract.BoolType,
		append(transferParams, manifest.NewParameter("data", smartcontract.AnyType))...,
	)
	md = NewMethodAndPrice(n.Transfer, 1<<17, callflag.States|callflag.AllowCall|callflag.AllowNotify)
	md.StorageFee = 50
	n.AddMethod(md, desc)

	eDesc := NewEventDescriptor("Transfer", transferParams...)
	eMD := NewEvent(eDesc)
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
	_, supply := c.getTotalSupply(ic.DAO)
	return stackitem.NewBigInteger(supply)
}

func (c *nep17TokenNative) getTotalSupply(d *dao.Simple) (state.StorageItem, *big.Int) {
	si := d.GetStorageItem(c.ID, totalSupplyKey)
	if si == nil {
		si = []byte{}
	}
	return si, bigint.FromBytes(si)
}

func (c *nep17TokenNative) Transfer(ic *interop.Context, args []stackitem.Item) stackitem.Item {
	return stackitem.NewBool(true)
}

func addrToStackItem(u *util.Uint160) stackitem.Item {
	if u == nil {
		return stackitem.Null{}
	}
	return stackitem.NewByteArray(u.BytesBE())
}

func (c *nep17TokenNative) postTransfer(ic *interop.Context, from, to *util.Uint160, amount *big.Int,
	data stackitem.Item, callOnPayment bool, postCalls ...func()) {
}

func (c *nep17TokenNative) emitTransfer(ic *interop.Context, from, to *util.Uint160, amount *big.Int) error {
	return ic.AddNotification(c.Hash, "Transfer", stackitem.NewArray([]stackitem.Item{
		addrToStackItem(from),
		addrToStackItem(to),
		stackitem.NewBigInteger(amount),
	}))
}

// TransferInternal transfers NEO across accounts.
func (c *nep17TokenNative) TransferInternal(ic *interop.Context, from, to util.Uint160, amount *big.Int, data stackitem.Item) error {
	return nil
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
	return
}

func (c *nep17TokenNative) Burn(ic *interop.Context, h util.Uint160, amount *big.Int) {
	return
}

func NewDescriptor(name string, ret smartcontract.ParamType, ps ...manifest.Parameter) *manifest.Method {
	if len(ps) == 0 {
		ps = []manifest.Parameter{}
	}
	return &manifest.Method{
		Name:       name,
		Parameters: ps,
		ReturnType: ret,
	}
}

// NewMethodAndPrice builds method with the provided descriptor and ActiveFrom/ActiveTill hardfork
// values consequently specified via activations. [config.HFDefault] specified as ActiveFrom is treated
// as active starting from the genesis block.
func NewMethodAndPrice(f interop.Method, cpuFee int64, flags callflag.CallFlag, activations ...config.Hardfork) *interop.MethodAndPrice {
	md := &interop.MethodAndPrice{
		HFSpecificMethodAndPrice: interop.HFSpecificMethodAndPrice{
			Func:          f,
			CPUFee:        cpuFee,
			RequiredFlags: flags,
		},
	}
	if len(activations) > 0 {
		if activations[0] != config.HFDefault {
			md.ActiveFrom = &activations[0]
		}
	}
	if len(activations) > 1 {
		md.ActiveTill = &activations[1]
	}
	return md
}

func NewEventDescriptor(name string, ps ...manifest.Parameter) *manifest.Event {
	if len(ps) == 0 {
		ps = []manifest.Parameter{}
	}
	return &manifest.Event{
		Name:       name,
		Parameters: ps,
	}
}

// NewEvent builds event with the provided descriptor and ActiveFrom/ActiveTill hardfork
// values consequently specified via activations.
func NewEvent(desc *manifest.Event, activations ...config.Hardfork) interop.Event {
	md := interop.Event{
		HFSpecificEvent: interop.HFSpecificEvent{
			MD: desc,
		},
	}
	if len(activations) > 0 {
		md.ActiveFrom = &activations[0]
	}
	if len(activations) > 1 {
		md.ActiveTill = &activations[1]
	}
	return md
}

// makeUint160Key creates a key from the account script hash.
func makeUint160Key(prefix byte, h util.Uint160) []byte {
	k := make([]byte, util.Uint160Size+1)
	k[0] = prefix
	copy(k[1:], h.BytesBE())
	return k
}
