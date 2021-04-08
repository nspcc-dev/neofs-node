package common

import (
	"encoding/binary"
)

var (
	auditPrefix                   = []byte{0x40}
	basicIncomeCollectionPrefix   = []byte{0x41}
	basicIncomeDistributionPrefix = []byte{0x42}
)

func AuditSettlementDetails(epoch uint64) []byte {
	return details(auditPrefix, epoch)
}

func BasicIncomeCollectionDetails(epoch uint64) []byte {
	return details(basicIncomeCollectionPrefix, epoch)
}

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
