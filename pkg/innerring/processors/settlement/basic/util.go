package basic

import (
	"math/big"
)

// NodeSizeTable is not thread safe, make sure it is accessed with external
// locks or in single routine.
type NodeSizeTable struct {
	prices map[string]uint64
	total  uint64
}

func (t *NodeSizeTable) Put(id []byte, avg uint64) {
	t.prices[string(id)] += avg
	t.total += avg
}

func (t *NodeSizeTable) Total() *big.Int {
	return big.NewInt(0).SetUint64(t.total)
}

func (t *NodeSizeTable) Iterate(f func([]byte, *big.Int)) {
	for k, v := range t.prices {
		n := big.NewInt(0).SetUint64(v)
		f([]byte(k), n)
	}
}

func NewNodeSizeTable() *NodeSizeTable {
	return &NodeSizeTable{
		prices: make(map[string]uint64),
	}
}

type nodeInfoWrapper []byte

func (nodeInfoWrapper) Price() *big.Int {
	panic("should not be used")
}

func (n nodeInfoWrapper) PublicKey() []byte {
	return n
}
