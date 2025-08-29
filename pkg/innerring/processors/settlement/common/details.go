package common

import (
	"encoding/binary"
)

var (
	basicIncomeDistributionPrefix = []byte{0x42}
)

func BasicIncomeDistributionDetails(epoch uint64) []byte {
	return details(basicIncomeDistributionPrefix, epoch)
}

func details(prefix []byte, epoch uint64) []byte {
	prefixLen := len(prefix)
	buf := make([]byte, prefixLen+8)

	copy(buf, prefix)
	binary.LittleEndian.PutUint64(buf[prefixLen:], epoch)

	return buf
}
