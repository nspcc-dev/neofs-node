package audit

import (
	"math/big"

	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
)

type transferTable struct {
	txs map[string]map[string]*transferTx
}

type transferTx struct {
	from, to *owner.ID

	amount *big.Int
}

func newTransferTable() *transferTable {
	return &transferTable{
		txs: make(map[string]map[string]*transferTx),
	}
}

func (t *transferTable) transfer(tx *transferTx) {
	from, to := tx.from.String(), tx.to.String()
	if from == to {
		return
	}

	m, ok := t.txs[from]
	if !ok {
		if m, ok = t.txs[to]; ok {
			to = from // ignore `from = to` swap because `from` doesn't require
			tx.amount.Neg(tx.amount)
		} else {
			m = make(map[string]*transferTx, 1)
			t.txs[from] = m
		}
	}

	tgt, ok := m[to]
	if !ok {
		m[to] = tx
		return
	}

	tgt.amount.Add(tgt.amount, tx.amount)
}

func (t *transferTable) iterate(f func(*transferTx)) {
	for _, m := range t.txs {
		for _, tx := range m {
			f(tx)
		}
	}
}
