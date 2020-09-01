package invoke

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	// TransferXParams for TransferBalanceX invocation.
	TransferXParams struct {
		Sender   []byte
		Receiver []byte
		Amount   int64 // in Fixed16
		Comment  []byte
	}

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
)

// TransferBalanceX invokes transferX method.
func TransferBalanceX(cli *client.Client, con util.Uint160, p *TransferXParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, extraFee, transferXMethod,
		p.Sender,
		p.Receiver,
		p.Amount,
		p.Comment,
	)
}

// Mint assets in contract.
func Mint(cli *client.Client, con util.Uint160, p *MintBurnParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, extraFee, mintMethod,
		p.ScriptHash,
		p.Amount,
		p.Comment,
	)
}

// Burn minted assets.
func Burn(cli *client.Client, con util.Uint160, p *MintBurnParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, extraFee, burnMethod,
		p.ScriptHash,
		p.Amount,
		p.Comment,
	)
}

// LockAsset invokes Lock method.
func LockAsset(cli *client.Client, con util.Uint160, p *LockParams) error {
	if cli == nil {
		return client.ErrNilClient
	}

	return cli.Invoke(con, extraFee, lockMethod,
		p.ID,
		p.User.BytesBE(),
		p.LockAccount.BytesBE(),
		p.Amount,
		int64(p.Until), // fixme: invoke can work only with int64 values
	)
}
