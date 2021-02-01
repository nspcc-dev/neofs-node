package common

import (
	"math/big"

	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
)

type TransferTable struct {
	txs map[string]map[string]*TransferTx
}

type TransferTx struct {
	From, To *owner.ID

	Amount *big.Int
}

func NewTransferTable() *TransferTable {
	return &TransferTable{
		txs: make(map[string]map[string]*TransferTx),
	}
}

func (t *TransferTable) Transfer(tx *TransferTx) {
	from, to := tx.From.String(), tx.To.String()
	if from == to {
		return
	}

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

func TransferAssets(e Exchanger, t *TransferTable) {
	t.Iterate(func(tx *TransferTx) {
		sign := tx.Amount.Sign()
		if sign == 0 {
			return
		}

		if sign < 0 {
			tx.From, tx.To = tx.To, tx.From
			tx.Amount.Neg(tx.Amount)
		}

		e.Transfer(tx.From, tx.To, tx.Amount)
	})
}
