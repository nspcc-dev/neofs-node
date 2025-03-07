package meta

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

const (
	// raw storage prefixes.

	// rootKey is the key for the last known state root in KV data base
	// associated with MPT.
	rootKey = 0x00
)

// NeoFSNetwork describes current NeoFS storage network state.
type NeoFSNetwork interface {
	// List returns node's containers that support chain-based meta data and
	// any error that does not allow listing.
	List() (map[cid.ID]struct{}, error)
	// IsMineWithMeta checks if the given CID has meta enabled and current
	// node belongs to it.
	IsMineWithMeta(cid.ID) (bool, error)
	// Head returns actual object header from the NeoFS network (non-local
	// objects should also be returned). Missing, removed object statuses
	// must be reported according to API statuses from SDK.
	Head(context.Context, cid.ID, oid.ID) (object.Object, error)
}

// wsClient is for test purposes only.
type wsClient interface {
	GetBlockNotifications(blockHash util.Uint256, filters ...*neorpc.NotificationFilter) (*result.BlockNotifications, error)
	GetVersion() (*result.Version, error)

	ReceiveHeadersOfAddedBlocks(flt *neorpc.BlockFilter, rcvr chan<- *block.Header) (string, error)
	ReceiveExecutionNotifications(flt *neorpc.NotificationFilter, rcvr chan<- *state.ContainedNotificationEvent) (string, error)
	Unsubscribe(id string) error

	Close()
}

// Meta handles object meta information received from FS chain and object
// storages. Chain information is stored in Merkle-Patricia Tries. Full objects
// index is built and stored as a simple KV storage.
type Meta struct {
	l        *zap.Logger
	rootPath string
	netmapH  util.Uint160
	cnrH     util.Uint160
	net      NeoFSNetwork

	stM      sync.RWMutex
	storages map[cid.ID]*containerStorage

	timeout     time.Duration
	magicNumber uint32
	cliM        sync.RWMutex
	ws          wsClient
	blockSubID  string
	bCh         chan *block.Header
	cnrDelEv    chan *state.ContainedNotificationEvent
	cnrPutEv    chan *state.ContainedNotificationEvent
	epochEv     chan *state.ContainedNotificationEvent

	blockBuff chan *block.Header

	// runtime reload fields
	cfgM      sync.RWMutex
	endpoints []string
}

const blockBuffSize = 1024

// Parameters groups arguments for [New] call.
type Parameters struct {
	Logger        *zap.Logger
	Network       NeoFSNetwork
	Timeout       time.Duration
	ContainerHash util.Uint160
	NetmapHash    util.Uint160
	RootPath      string

	// fields that support runtime reload
	NeoEnpoints []string
}

// New makes [Meta].
func New(p Parameters) (*Meta, error) {
	storagesFS, err := os.ReadDir(p.RootPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read existing container storages: %w", err)
	}
	storagesRead := make(map[cid.ID]*containerStorage)
	for _, s := range storagesFS {
		sName := s.Name()
		cID, err := cid.DecodeString(sName)
		if err != nil {
			p.Logger.Warn("skip unknown container storage entity", zap.String("name", sName), zap.Error(err))
			continue
		}

		st, err := storageForContainer(p.RootPath, cID)
		if err != nil {
			p.Logger.Warn("skip container storage that cannot be read", zap.String("name", sName), zap.Error(err))
			continue
		}

		storagesRead[cID] = st
	}

	storages := storagesRead
	defer func() {
		if err != nil {
			for _, st := range storages {
				_ = st.db.Close()
			}
		}
	}()

	cnrsNetwork, err := p.Network.List()
	if err != nil {
		return nil, fmt.Errorf("listing node's containers: %w", err)
	}
	for cID := range storagesRead {
		if _, ok := cnrsNetwork[cID]; !ok {
			err = storagesRead[cID].drop()
			if err != nil {
				p.Logger.Warn("could not drop container storage", zap.Stringer("cID", cID), zap.Error(err))
			}

			delete(storagesRead, cID)
		}
	}

	for cID := range cnrsNetwork {
		if _, ok := storages[cID]; !ok {
			st, err := storageForContainer(p.RootPath, cID)
			if err != nil {
				return nil, fmt.Errorf("open container storage %s: %w", cID, err)
			}

			storages[cID] = st
		}
	}

	return &Meta{
		l:         p.Logger,
		rootPath:  p.RootPath,
		netmapH:   p.NetmapHash,
		cnrH:      p.ContainerHash,
		net:       p.Network,
		endpoints: p.NeoEnpoints,
		timeout:   p.Timeout,
		bCh:       make(chan *block.Header),
		cnrDelEv:  make(chan *state.ContainedNotificationEvent),
		cnrPutEv:  make(chan *state.ContainedNotificationEvent),
		epochEv:   make(chan *state.ContainedNotificationEvent),
		blockBuff: make(chan *block.Header, blockBuffSize),
		storages:  storages}, nil
}

// Reload updates service in runtime.
// Currently supported fields:
//   - endpoints
func (m *Meta) Reload(p Parameters) error {
	m.cfgM.Lock()
	defer m.cfgM.Unlock()

	m.endpoints = p.NeoEnpoints

	return nil
}

// Run starts notification handling. Must be called only on instances created
// with [New]. Blocked until context is done.
func (m *Meta) Run(ctx context.Context) error {
	defer func() {
		m.stM.Lock()
		for _, st := range m.storages {
			st.m.Lock()
			_ = st.db.Close()
			st.m.Unlock()
		}
		maps.Clear(m.storages)

		m.stM.Unlock()
	}()

	var err error
	m.ws, err = m.connect(ctx)
	if err != nil {
		return fmt.Errorf("connect to NEO RPC: %w", err)
	}
	defer m.ws.Close()

	v, err := m.ws.GetVersion()
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}
	m.magicNumber = uint32(v.Protocol.Network)

	m.stM.RLock()
	hasContainers := len(m.storages) > 0
	m.stM.RUnlock()

	if hasContainers {
		m.blockSubID, err = m.subscribeForBlocks(m.bCh)
		if err != nil {
			return fmt.Errorf("block subscription: %w", err)
		}
	}

	err = m.subscribeForMeta()
	if err != nil {
		return fmt.Errorf("subscribe for meta notifications: %w", err)
	}

	go m.flusher(ctx)
	go m.blockFetcher(ctx, m.blockBuff)

	return m.listenNotifications(ctx)
}

func (m *Meta) flusher(ctx context.Context) {
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
