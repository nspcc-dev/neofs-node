package node

import (
	"strconv"
	"strings"

	"github.com/nspcc-dev/neofs-api-go/object"
)

const optionPrice = "/Price:"

const optionCapacity = "/Capacity:"

// Price parses node options and returns the price in 1e-8*GAS/Megabyte per month.
//
// User sets the price in GAS/Terabyte per month.
func (i Info) Price() uint64 {
	for j := range i.opts {
		if strings.HasPrefix(i.opts[j], optionPrice) {
			n, err := strconv.ParseFloat(i.opts[j][len(optionPrice):], 64)
			if err != nil {
				return 0
			}

			return uint64(n*1e8) / uint64(object.UnitsMB) // UnitsMB == megabytes in 1 terabyte
		}
	}

	return 0
}

// Capacity parses node options and returns the capacity .
func (i Info) Capacity() uint64 {
	for j := range i.opts {
		if strings.HasPrefix(i.opts[j], optionCapacity) {
			n, err := strconv.ParseUint(i.opts[j][len(optionCapacity):], 10, 64)
			if err != nil {
				return 0
			}

			return n
		}
	}

	return 0
}
