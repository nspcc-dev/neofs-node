package meta

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// raceMetaChain is a MetaChain whose Subscribe* methods keep sending into
// the given channel as fast as possible, from a goroutine Run itself does
// not wait on - exactly like the real chain's internal delivery goroutine -
// until the matching Unsubscribe* method is called and confirmed to have
// stopped it. This lets a test deterministically exercise the race between
// Run's shutdown and the chain's own concurrent sends.
type raceMetaChain struct {
	stopBlocks chan struct{}
	blocksWG   sync.WaitGroup

	stopNotifs chan struct{}
	notifsWG   sync.WaitGroup
}

func (c *raceMetaChain) Magic() uint32                                     { return 0 }
func (c *raceMetaChain) Height() uint32                                    { return 0 }
func (c *raceMetaChain) AddTx(*transaction.Transaction) error              { panic("not used by Run") }
func (c *raceMetaChain) TransactionTestInvocation(*transaction.Transaction) error {
	panic("not used by Run")
}

func (c *raceMetaChain) SubscribeForBlocks(ch chan *block.Header) {
	stop := make(chan struct{})
	c.stopBlocks = stop
	c.blocksWG.Add(1)
	go func() {
		defer c.blocksWG.Done()
		for {
			select {
			case <-stop:
				return
			case ch <- new(block.Header):
			}
		}
	}()
}

func (c *raceMetaChain) UnsubscribeFromBlocks(chan *block.Header) {
	close(c.stopBlocks)
	c.blocksWG.Wait() // only returns once the sender goroutine is guaranteed gone
}

func (c *raceMetaChain) SubscribeForNotifications(ch chan *state.ContainedNotificationEvent) {
	stop := make(chan struct{})
	c.stopNotifs = stop
	c.notifsWG.Add(1)
	go func() {
		defer c.notifsWG.Done()
		for {
			select {
			case <-stop:
				return
			case ch <- new(state.ContainedNotificationEvent):
			}
		}
	}()
}

func (c *raceMetaChain) UnsubscribeFromNotifications(chan *state.ContainedNotificationEvent) {
	close(c.stopNotifs)
	c.notifsWG.Wait()
}

// Regression test for M43: Run used to close m.bCh/m.evsCh right after its
// own handler goroutines stopped, without telling the chain to stop sending
// into them first. The chain's delivery goroutine runs independently and
// keeps sending as long as it's subscribed, so a send racing the close
// panics with "send on closed channel". Cancelling the context immediately,
// with the fake chain hammering both channels as fast as possible, reliably
// hits that window if Run doesn't unsubscribe before closing.
func TestMeta_Run_DoesNotPanicOnConcurrentChainSends(t *testing.T) {
	m, err := New(Parameters{
		Logger:  zaptest.NewLogger(t),
		Chain:   &raceMetaChain{},
		Path:    t.TempDir(),
		Network: noopNetwork{},
	})
	require.NoError(t, err)
	// addMetrics registers collectors in the global default registerer;
	// unregister them so other tests in this package can call New again.
	t.Cleanup(func() {
		prometheus.Unregister(m.metrics.newBlockFetchTime)
		prometheus.Unregister(m.metrics.objAcceptTime)
		prometheus.Unregister(m.metrics.objAcceptBlocks)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	require.NotPanics(t, func() {
		require.NoError(t, m.Run(ctx))
	})
}
