package rand

import (
	crand "crypto/rand"
	"encoding/binary"
	mrand "math/rand"
)

type cryptoSource struct{}

// Read is alias for crypto/rand.Read.
var Read = crand.Read

// New constructs the source of random numbers.
func New() *mrand.Rand {
	return mrand.New(&cryptoSource{})
}

func (s *cryptoSource) Seed(int64) {}

func (s *cryptoSource) Int63() int64 {
	return int64(s.Uint63())
}

func (s *cryptoSource) Uint63() uint64 {
	buf := make([]byte, 8)
	if _, err := crand.Read(buf); err != nil {
		return 0
	}

	return binary.BigEndian.Uint64(buf)
}

// Uint64 returns a random uint64 value.
func Uint64(r *mrand.Rand, max int64) uint64 {
	if max <= 0 {
		return 0
	}

	var i int64 = -1
	for i < 0 {
		i = r.Int63n(max)
	}

	return uint64(i)
}
