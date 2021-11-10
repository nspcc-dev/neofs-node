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

	a := container.NewAnnouncement()
	a.SetContainerID(cidtest.GenerateID())
	a.SetEpoch(epoch)

	const opinionsNum = 100

	s := New(Prm{})

	opinions := make([]uint64, opinionsNum)
	for i := range opinions {
		opinions[i] = rand.Uint64()

		a.SetUsedSpace(opinions[i])

		require.NoError(t, s.Put(*a))
	}

	iterCounter := 0

	err := s.Iterate(
		func(ai container.UsedSpaceAnnouncement) bool {
			return ai.Epoch() == epoch
		},
		func(ai container.UsedSpaceAnnouncement) error {
			iterCounter++

			require.Equal(t, epoch, ai.Epoch())
			require.Equal(t, a.ContainerID(), ai.ContainerID())
			require.Equal(t, finalEstimation(opinions), ai.UsedSpace())

			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, iterCounter)
}
