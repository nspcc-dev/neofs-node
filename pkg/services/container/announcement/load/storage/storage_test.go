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

	s := New(0)

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

func TestStorage_NewEpoch(t *testing.T) {
	const epoch uint64 = 13
	const lifeCycle = 5

	s := New(lifeCycle)

	var a container.SizeEstimation
	a.SetContainer(cidtest.ID())
	a.SetEpoch(epoch)

	err := s.Put(a)
	require.NoError(t, err)

	for i := uint64(1); i <= lifeCycle+1; i++ {
		ee := getEstimations(t, s)
		require.NoError(t, err)
		require.Len(t, ee, 1)

		s.EpochEvent(epoch + i)
	}

	ee := getEstimations(t, s)
	require.NoError(t, err)
	require.Empty(t, ee)
}

func getEstimations(t *testing.T, s *Storage) []container.SizeEstimation {
	var ee []container.SizeEstimation

	err := s.Iterate(
		func(e container.SizeEstimation) bool {
			return true
		},
		func(e container.SizeEstimation) error {
			ee = append(ee, e)
			return nil
		},
	)
	require.NoError(t, err)

	return ee
}
