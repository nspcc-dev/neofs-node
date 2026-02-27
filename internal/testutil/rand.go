package testutil

import (
	"encoding/binary"
	"math/rand/v2"
	"time"
)

// RandByteSlice returns randomized byte slice of specified length.
func RandByteSlice[I ~int | ~uint64](ln I) []byte {
	var seed [32]byte
	binary.LittleEndian.PutUint64(seed[:], uint64(time.Now().UnixNano()))
	b := make([]byte, ln)
	_, _ = rand.NewChaCha8(seed).Read(b) // docs say never returns an error
	return b
}
