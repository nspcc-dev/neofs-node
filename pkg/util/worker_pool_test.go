package util

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncWorkerPool(t *testing.T) {
	t.Run("submit to released pool", func(t *testing.T) {
		p := NewPseudoWorkerPool()
		p.Release()
		require.Equal(t, ErrPoolClosed, p.Submit(func() {}))
	})
	t.Run("create and wait", func(t *testing.T) {
		p := NewPseudoWorkerPool()
		ch1, ch2 := make(chan struct{}), make(chan struct{})
		wg := new(sync.WaitGroup)
		wg.Add(2)
		go func(t *testing.T) {
			defer wg.Done()
			err := p.Submit(newControlledReturnFunc(ch1))
			require.NoError(t, err)
		}(t)
		go func(t *testing.T) {
			defer wg.Done()
			err := p.Submit(newControlledReturnFunc(ch2))
			require.NoError(t, err)
		}(t)

		// Make sure functions were submitted.
		<-ch1
		<-ch2
		p.Release()
		require.Equal(t, ErrPoolClosed, p.Submit(func() {}))

		close(ch1)
		close(ch2)
		wg.Wait()
	})
}

// newControlledReturnFunc returns function which signals in ch after
// it has started and waits for some value in channel to return.
// ch must be unbuffered.
func newControlledReturnFunc(ch chan struct{}) func() {
	return func() {
		ch <- struct{}{}
		<-ch
	}
}
