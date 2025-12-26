package gas

import (
	"errors"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/dao"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativeids"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

// DefaultBalance is a balance of every account in redefined [GAS] native
// contract
const DefaultBalance = 100

// GAS represents GAS custom native contract. It always returns [DefaultBalance] as a
// balance, has no-op `Burn`, `Mint`, `Transfer` operations.
type GAS struct {
	nep17TokenNative
	initialSupply int64
}

// NewGAS returns [GAS] custom native contract.
func NewGAS(init int64) *GAS {
	g := &GAS{
		initialSupply: init,
	}
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
	if hf != g.ActiveIn() {
		return nil
	}

	if err := g.nep17TokenNative.Initialize(ic); err != nil {
		return err
	}
	_, totalSupply := g.getTotalSupply(ic.DAO)
	if totalSupply.Sign() != 0 {
		return errors.New("already initialized")
	}
	h, err := getStandbyValidatorsHash(ic)
	if err != nil {
		return err
	}
	g.Mint(ic, h, big.NewInt(g.initialSupply), false)
	return nil
}

// InitializeCache implements the Contract interface.
func (g *GAS) InitializeCache(_ interop.IsHardforkEnabled, blockHeight uint32, d *dao.Simple) error {
	return nil
}

// OnPersist implements the Contract interface.
func (g *GAS) OnPersist(ic *interop.Context) error {
	return nil
}

// PostPersist implements the Contract interface.
func (g *GAS) PostPersist(ic *interop.Context) error {
	return nil
}

// ActiveIn implements the Contract interface.
func (g *GAS) ActiveIn() *config.Hardfork {
	return nil
}

// BalanceOf returns native GAS token balance for the acc.
func (g *GAS) BalanceOf(d *dao.Simple, acc util.Uint160) *big.Int {
	return g.balanceOfInternal(d, acc)
}

func getStandbyValidatorsHash(ic *interop.Context) (util.Uint160, error) {
	cfg := ic.Chain.GetConfig()
	committee, err := keys.NewPublicKeysFromStrings(cfg.StandbyCommittee)
	if err != nil {
		return util.Uint160{}, err
	}
	s, err := smartcontract.CreateDefaultMultiSigRedeemScript(committee[:cfg.GetNumOfCNs(0)])
	if err != nil {
		return util.Uint160{}, err
	}
	return hash.Hash160(s), nil
}
