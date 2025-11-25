package container

import (
	"fmt"
	"math"
	"math/big"
)

func toUint32(n *big.Int) (uint32, error) {
	if !n.IsUint64() {
		return 0, fmt.Errorf("%s is not a valid uint32", n)
	}

	u64 := n.Uint64()
	if u64 > math.MaxUint32 {
		return 0, fmt.Errorf("%s is not a valid uint32", n)
	}

	return uint32(u64), nil
}
