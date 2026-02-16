package meta

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/util"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// notificationBuffSize is a nesessary buffer for neo-go's client proper
	// notification work; it is required to always read notifications without
	// any blocking or making additional RPC.
	notificationBuffSize = 100
)

// wsClient is for test purposes only.
type wsClient interface {
	invoker.RPCInvoke

	GetBlockNotifications(blockHash util.Uint256, filters *neorpc.NotificationFilter) (*result.BlockNotifications, error)
	GetVersion() (*result.Version, error)

	ReceiveHeadersOfAddedBlocks(flt *neorpc.BlockFilter, rcvr chan<- *block.Header) (string, error)
	ReceiveExecutionNotifications(flt *neorpc.NotificationFilter, rcvr chan<- *state.ContainedNotificationEvent) (string, error)
	Unsubscribe(id string) error

	Close()
}

func newNotifier() objectNotifier {
	return objectNotifier{
		notifications: make(chan oid.Address, 1024),
		subs:          make(map[oid.Address]chan<- struct{}),
	}
}

type objectNotifier struct {
	notifications chan oid.Address

	m    sync.Mutex
	subs map[oid.Address]chan<- struct{}
}

func (on *objectNotifier) subscribe(addr oid.Address, ch chan<- struct{}) {
	on.m.Lock()
	defer on.m.Unlock()

	on.subs[addr] = ch
}

func (on *objectNotifier) unsubscribe(addr oid.Address) {
	on.m.Lock()
	defer on.m.Unlock()

	delete(on.subs, addr)
}

func (on *objectNotifier) notifyReceived(addr oid.Address) {
	on.m.Lock()
	defer on.m.Unlock()

	ch, ok := on.subs[addr]
	if ok {
		close(ch)
		delete(on.subs, addr)
	}
}

// Meta handles object meta information received from FS chain and object
// storages. Chain information is stored in Merkle-Patricia Tries. Full objects
// index is built and stored as a simple KV storage.
type Meta struct {
	l *zap.Logger

	ws          wsClient
	magicNumber uint32
	blockSubID  string
	cnrSubID    string
	cnrCrtSubID string
	bCh         chan *block.Header
	cnrPutEv    chan *state.ContainedNotificationEvent
	epochEv     chan *state.ContainedNotificationEvent

	notifier objectNotifier

	blockHeadersBuff chan *block.Header
	blockEventsBuff  chan blockObjEvents
}

const blockBuffSize = 1024

// Parameters groups arguments for [New] call.
type Parameters struct {
	Logger  *zap.Logger
	MetaCli *rpcclient.WSClient
}

func validatePrm(p Parameters) error {
	if p.Logger == nil {
		return errors.New("missing logger")
	}
	if p.MetaCli == nil {
		return errors.New("missing meta client")
	}

	return nil
}

// New makes [Meta].
func New(p Parameters) (*Meta, error) {
	err := validatePrm(p)
	if err != nil {
		return nil, err
	}

	return &Meta{
		l:                p.Logger,
		ws:               p.MetaCli,
		bCh:              make(chan *block.Header, notificationBuffSize),
		blockHeadersBuff: make(chan *block.Header, blockBuffSize),
		blockEventsBuff:  make(chan blockObjEvents, blockBuffSize),
		notifier:         newNotifier(),
	}, nil
}

// Run starts notification handling. Must be called only on instances created
// with [New]. Blocked until context is done.
func (m *Meta) Run(ctx context.Context) error {
	v, err := m.ws.GetVersion()
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}
	m.magicNumber = uint32(v.Protocol.Network)

	var wg sync.WaitGroup
	wg.Add(1)

	go m.blockHandler(ctx, m.blockHeadersBuff, &wg)

	err = m.listenNotifications(ctx)
	wg.Wait()

	return err
}

func (m *Meta) flusher(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	const (
		flushInterval = time.Second
		collapseDepth = 10
	)

	t := time.NewTicker(flushInterval)

	for {
		select {
		case <-t.C:
			m.stM.RLock()

			var wg errgroup.Group
			wg.SetLimit(1024)

			for _, st := range m.storages {
				if st == nil {
					panic(fmt.Errorf("nil container storage: %s", st.path))
				}

				wg.Go(func() error {
					st.m.Lock()
					defer st.m.Unlock()

					st.mpt.Collapse(collapseDepth)

					_, err := st.mpt.Store.PersistSync()
					if err != nil {
						return fmt.Errorf("persisting %q storage: %w", st.path, err)
					}

					return nil
				})
			}

			err := wg.Wait()

			m.stM.RUnlock()

			if err != nil {
				m.l.Error("storage flusher failed", zap.Error(err))
				continue
			}

			t.Reset(flushInterval)
		case <-ctx.Done():
			return
		}
	}
}
