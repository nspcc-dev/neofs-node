package meta

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// raceMetaChain sends into the subscribed channels as fast as possible from
// goroutines Run does not wait on, like the real chain's delivery goroutine,
// and records a panic instead of taking the test binary down with it.
type raceMetaChain struct {
	stop     chan struct{}
	wg       sync.WaitGroup
	panicked atomic.Pointer[string]
}

func newRaceMetaChain(t *testing.T) *raceMetaChain {
	c := &raceMetaChain{stop: make(chan struct{})}
	t.Cleanup(func() {
		close(c.stop)
		c.wg.Wait()
	})

	return c
}

func (c *raceMetaChain) Magic() uint32                        { return 0 }
func (c *raceMetaChain) Height() uint32                       { return 0 }
func (c *raceMetaChain) AddTx(*transaction.Transaction) error { panic("not used by Run") }
func (c *raceMetaChain) TransactionTestInvocation(*transaction.Transaction) error {
	panic("not used by Run")
}

// send keeps calling f until the test is over, recording any panic f causes.
func (c *raceMetaChain) send(f func()) {
	c.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				s := fmt.Sprint(r)
				c.panicked.CompareAndSwap(nil, &s)
			}
		}()
		for {
			select {
			case <-c.stop:
				return
			default:
				f()
			}
		}
	})
}

func (c *raceMetaChain) SubscribeForBlocks(ch chan *block.Header) {
	c.send(func() {
		select {
		case <-c.stop:
		case ch <- new(block.Header):
		}
	})
}

func (c *raceMetaChain) SubscribeForNotifications(ch chan *state.ContainedNotificationEvent) {
	c.send(func() {
		select {
		case <-c.stop:
		case ch <- new(state.ContainedNotificationEvent):
		}
	})
}

// Run must leave the channels the chain sends into open: closing one while the
// chain is still delivering panics that delivery goroutine with "send on
// closed channel", taking the node down on shutdown.
func TestMeta_Run_DoesNotCloseChainFedChannels(t *testing.T) {
	// addMetrics registers collectors in the global default registerer, which
	// another New in this package has already used; give this one its own.
	defaultReg := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = defaultReg })

	chain := newRaceMetaChain(t)
	m, err := New(Parameters{
		Logger:  zaptest.NewLogger(t),
		Chain:   chain,
		Path:    t.TempDir(),
		Network: noopNetwork{},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	require.NoError(t, m.Run(ctx))

	// Run has returned, so any close it performs has already happened; give
	// the senders time to hit it.
	require.Never(t, func() bool {
		return chain.panicked.Load() != nil
	}, 50*time.Millisecond, time.Millisecond,
		"chain delivery goroutine panicked with \"send on closed channel\"")
}
