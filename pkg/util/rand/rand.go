package rand

import (
	crand "crypto/rand"
	"encoding/binary"
	mrand "math/rand"
)

var source = mrand.New(&cryptoSource{})

// Uint64 returns a random uint64 value.
func Uint64() uint64 {
	return source.Uint64()
}

// Uint64 returns a random uint32 value.
func Uint32() uint32 {
	return source.Uint32()
}

// Shuffle randomizes the order of elements.
// n is the number of elements. Shuffle panics if n < 0.
// swap swaps the elements with indexes i and j.
func Shuffle(n int, swap func(i, j int)) {
	source.Shuffle(n, swap)
}

// cryptoSource is math/rand.Source which takes entropy via crypto/rand.
type cryptoSource struct{}

// Seed implements math/rand.Source.
func (s *cryptoSource) Seed(int64) {}

// Int63 implements math/rand.Source.
func (s *cryptoSource) Int63() int64 {
	return int64(s.Uint64() >> 1)
}

// Uint64 implements math/rand.Source64.
func (s *cryptoSource) Uint64() uint64 {
	var buf [8]byte
	_, _ = crand.Read(buf[:]) // always returns nil
	return binary.BigEndian.Uint64(buf[:])
}
