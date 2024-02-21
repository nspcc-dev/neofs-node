package putsvc

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetPayload(t *testing.T) {
	for i := 0; i < 100; i++ {
		cp := rand.Int() % 1024

		b := getBuffer(cp)
		require.GreaterOrEqual(t, cap(b), cp)

		putBuffer(b)
	}
}
