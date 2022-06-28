package loadstorage

import (
	"math/rand"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/container"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	const epoch uint64 = 13

	var a container.SizeEstimation
	a.SetContainer(cidtest.ID())
	a.SetEpoch(epoch)

	const opinionsNum = 100

	s := New(Prm{})

	opinions := make([]uint64, opinionsNum)
	for i := range opinions {
		opinions[i] = rand.Uint64()

		a.SetValue(opinions[i])

		require.NoError(t, s.Put(a))
	}

	iterCounter := 0

	err := s.Iterate(
		func(ai container.SizeEstimation) bool {
			return ai.Epoch() == epoch
		},
		func(ai container.SizeEstimation) error {
			iterCounter++

			require.Equal(t, epoch, ai.Epoch())
			require.Equal(t, a.Container(), ai.Container())
			require.Equal(t, finalEstimation(opinions), ai.Value())

			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, iterCounter)
}
