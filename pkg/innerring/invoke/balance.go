package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	// LockParams for LockAsset invocation.
	LockParams struct {
		ID          []byte
		User        util.Uint160
		LockAccount util.Uint160
		Amount      int64  // in Fixed16
		Until       uint64 // epochs
	}

	// MintBurnParams for Mint and Burn invocations.
	MintBurnParams struct {
		ScriptHash []byte
		Amount     int64 // in Fixed16
		Comment    []byte
	}
)

const (
	transferXMethod = "transferX"
	lockMethod      = "lock"
	mintMethod      = "mint"
	burnMethod      = "burn"
	precisionMethod = "decimals"
)

// Mint assets in contract.
func Mint(cli *client.Client, con util.Uint160, fee SideFeeProvider, p *MintBurnParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), mintMethod,
		p.ScriptHash,
		p.Amount,
		p.Comment,
	)
}

// Burn minted assets.
func Burn(cli *client.Client, con util.Uint160, fee SideFeeProvider, p *MintBurnParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), burnMethod,
		p.ScriptHash,
		p.Amount,
		p.Comment,
	)
}

// LockAsset invokes Lock method.
func LockAsset(cli *client.Client, con util.Uint160, fee SideFeeProvider, p *LockParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.NotaryInvoke(con, fee.SideChainFee(), lockMethod,
		p.ID,
		p.User.BytesBE(),
		p.LockAccount.BytesBE(),
		p.Amount,
		int64(p.Until),
	)
}

// BalancePrecision invokes Decimal method that returns precision of NEP-5 contract.
func BalancePrecision(cli *client.Client, con util.Uint160) (uint32, error) {
	if cli == nil {
		return 0, client.ErrNilClient
	}

	v, err := cli.TestInvoke(con, precisionMethod)
	if err != nil {
		return 0, err
	}

	precision, err := client.IntFromStackItem(v[0])
	if err != nil {
		return 0, err
	}

	return uint32(precision), nil
}
