package meta

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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

// ContainerLister is a source of actual containers current node belongs to.
type ContainerLister interface {
	// List returns node's containers that support chain-based meta data and
	// any error that does not allow listing.
	List() (map[cid.ID]struct{}, error)
}

// Meta handles object meta information received from FS chain and object
// storages. Chain information is stored in Merkle-Patricia Tries. Full objects
// index is built and stored as a simple KV storage.
type Meta struct {
	l        *zap.Logger
	rootPath string
	netmapH  util.Uint160
	cnrH     util.Uint160
	cLister  ContainerLister

	m        sync.RWMutex
	storages map[cid.ID]*containerStorage

	endpoints   []string
	timeout     time.Duration
	magicNumber uint32
	cliCtx      context.Context // for client context only, as it is required by the lib
	ws          *rpcclient.WSClient
	bCh         chan *block.Header
	objEv       chan *state.ContainedNotificationEvent
	cnrDelEv    chan *state.ContainedNotificationEvent
	cnrPutEv    chan *state.ContainedNotificationEvent
	epochEv     chan *state.ContainedNotificationEvent

	objNotificationBuff chan *state.ContainedNotificationEvent
}

const objsBufferSize = 1024

// New makes [Meta].
func New(l *zap.Logger, cLister ContainerLister, timeout time.Duration, endpoints []string, containerH, nmH util.Uint160, rootPath string) (*Meta, error) {
	storagesFS, err := os.ReadDir(rootPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read existing container storages: %w", err)
	}
	storagesRead := make(map[cid.ID]*containerStorage)
	for _, s := range storagesFS {
		sName := s.Name()
		cID, err := cid.DecodeString(sName)
		if err != nil {
			l.Warn("skip unknown container storage entity", zap.String("name", sName), zap.Error(err))
			continue
		}

		st, err := storageForContainer(rootPath, cID)
		if err != nil {
			l.Warn("skip container storage that cannot be read", zap.String("name", sName), zap.Error(err))
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

	cnrsNetwork, err := cLister.List()
	if err != nil {
		return nil, fmt.Errorf("listing node's containers: %w", err)
	}
	for cID := range storagesRead {
		if _, ok := cnrsNetwork[cID]; !ok {
			err = storagesRead[cID].drop()
			if err != nil {
				l.Warn("could not drop container storage", zap.Stringer("cID", cID), zap.Error(err))
			}

			delete(storagesRead, cID)
		}
	}

	for cID := range cnrsNetwork {
		if _, ok := storages[cID]; !ok {
			st, err := storageForContainer(rootPath, cID)
			if err != nil {
				return nil, fmt.Errorf("open container storage %s: %w", cID, err)
			}

			storages[cID] = st
		}
	}

	return &Meta{
		l:                   l,
		rootPath:            rootPath,
		netmapH:             nmH,
		cnrH:                containerH,
		cLister:             cLister,
		endpoints:           endpoints,
		timeout:             timeout,
		bCh:                 make(chan *block.Header),
		objEv:               make(chan *state.ContainedNotificationEvent),
		cnrDelEv:            make(chan *state.ContainedNotificationEvent),
		cnrPutEv:            make(chan *state.ContainedNotificationEvent),
		epochEv:             make(chan *state.ContainedNotificationEvent),
		objNotificationBuff: make(chan *state.ContainedNotificationEvent, objsBufferSize),
		storages:            storages}, nil
}

// Run starts notification handling. Must be called only on instances created
// with [New]. Blocked until context is done.
func (m *Meta) Run(ctx context.Context) error {
	defer func() {
		m.m.Lock()
		for _, st := range m.storages {
			st.m.Lock()
			_ = st.db.Close()
			st.m.Unlock()
		}
		maps.Clear(m.storages)

		m.m.Unlock()
	}()

	m.cliCtx = ctx

	var err error
	m.ws, err = m.connect()
	if err != nil {
		return fmt.Errorf("connect to NEO RPC: %w", err)
	}
	defer m.ws.Close()

	v, err := m.ws.GetVersion()
	if err != nil {
		return fmt.Errorf("get version: %w", err)
	}
	m.magicNumber = uint32(v.Protocol.Network)

	err = m.subscribeForMeta()
	if err != nil {
		return fmt.Errorf("subscribe for meta notifications: %w", err)
	}

	go m.flusher(ctx)
	go m.objNotificationWorker(ctx, m.objNotificationBuff)

	return m.listenNotifications(ctx)
}

func (m *Meta) flusher(ctx context.Context) {
	const flushInterval = time.Second

	t := time.NewTicker(flushInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			m.m.RLock()

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

			m.m.RUnlock()

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
