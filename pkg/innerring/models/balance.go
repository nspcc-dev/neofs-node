package models

import "github.com/nspcc-dev/neo-go/pkg/util"

type LockAssetsEvent struct {
	WithdrawTx  util.Uint256
	UserAccount util.Uint160
	Amount      uint64
}
