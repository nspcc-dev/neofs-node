package blobovnicza

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"strconv"
)

const firstBucketBound = uint64(32 * 1 << 10) // 32KB

func stringifyBounds(lower, upper uint64) string {
	return fmt.Sprintf("[%s:%s]",
		stringifyByteSize(lower),
		stringifyByteSize(upper),
	)
}

func stringifyByteSize(sz uint64) string {
	return strconv.FormatUint(sz, 10)
}

func bucketKeyFromBounds(upperBound uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)

	ln := binary.PutUvarint(buf, upperBound)

	return buf[:ln]
}

func bucketForSize(sz uint64) []byte {
	return bucketKeyFromBounds(upperPowerOfTwo(sz))
}

func upperPowerOfTwo(v uint64) uint64 {
	if v <= firstBucketBound {
		return firstBucketBound
	}

	return 1 << bits.Len64(v-1)
}

func (b *Blobovnicza) incSize(sz uint64) {
	b.filled.Add(sz)
}

func (b *Blobovnicza) decSize(sz uint64) {
	b.filled.Add(^(sz - 1))
}

func (b *Blobovnicza) full() bool {
	return b.filled.Load() >= b.fullSizeLimit
}
