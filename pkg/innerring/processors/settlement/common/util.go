package common

import (
	"math/big"

	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type TransferTable struct {
	txs map[string]map[string]*TransferTx
}

type TransferTx struct {
	From, To user.ID

	Amount *big.Int
}

func NewTransferTable() *TransferTable {
	return &TransferTable{
		txs: make(map[string]map[string]*TransferTx),
	}
}

func (t *TransferTable) Transfer(tx *TransferTx) {
	if tx.From.Equals(tx.To) {
		return
	}

	from, to := tx.From.EncodeToString(), tx.To.EncodeToString()

	m, ok := t.txs[from]
	if !ok {
		if m, ok = t.txs[to]; ok {
			to = from // ignore `From = To` swap because `From` doesn't require
			tx.Amount.Neg(tx.Amount)
		} else {
			m = make(map[string]*TransferTx, 1)
			t.txs[from] = m
		}
	}

	tgt, ok := m[to]
	if !ok {
		m[to] = tx
		return
	}

	tgt.Amount.Add(tgt.Amount, tx.Amount)
}

func (t *TransferTable) Iterate(f func(*TransferTx)) {
	for _, m := range t.txs {
		for _, tx := range m {
			f(tx)
		}
	}
}

func TransferAssets(e Exchanger, t *TransferTable, details []byte) {
	t.Iterate(func(tx *TransferTx) {
		sign := tx.Amount.Sign()
		if sign == 0 {
			return
		}

		if sign < 0 {
			tx.From, tx.To = tx.To, tx.From
			tx.Amount.Neg(tx.Amount)
		}

		e.Transfer(tx.From, tx.To, tx.Amount, details)
	})
}
