package testutil

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"time"

	"golang.org/x/exp/constraints"
)

// RandByteSlice returns randomized byte slice of specified length.
func RandByteSlice[I constraints.Integer](ln I) []byte {
	var seed [32]byte
	binary.LittleEndian.PutUint64(seed[:], uint64(time.Now().UnixNano()))
	b := make([]byte, ln)
	_, _ = rand.NewChaCha8(seed).Read(b) // docs say never returns an error
	return b
}

func TillContextDeadline(ctx context.Context) string {
	deadline, ok := ctx.Deadline()
	if ok {
		return time.Until(deadline).String()
	}
	return "INFINITY"
}
