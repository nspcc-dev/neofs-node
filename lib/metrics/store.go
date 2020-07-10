package metrics

import (
	"encoding/binary"
	"encoding/hex"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"go.uber.org/zap"
)

type (
	syncStore struct {
		log   *zap.Logger
		store core.Bucket
		mutex sync.RWMutex
		items map[refs.CID]uint64
	}

	// SpaceOp is an enumeration of space size operations.
	SpaceOp int
)

const (
	_ SpaceOp = iota

	// AddSpace is a SpaceOp of space size increasing.
	AddSpace

	// RemSpace is a SpaceOp of space size decreasing.
	RemSpace
)

func newSyncStore(log *zap.Logger, store core.Bucket) *syncStore {
	return &syncStore{
		log:   log,
		store: store,
		items: make(map[refs.CID]uint64),
	}
}

func (m *syncStore) Load() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_ = m.store.Iterate(func(key, val []byte) bool {
		cid, err := refs.CIDFromBytes(key)
		if err != nil {
			m.log.Error("could not load space value", zap.Error(err))
			return true
		}

		m.items[cid] += binary.BigEndian.Uint64(val)
		return true
	})
}

func (m *syncStore) Reset(items map[refs.CID]uint64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.items = items
	if items == nil {
		m.items = make(map[refs.CID]uint64)
	}

	keys, err := m.store.List()
	if err != nil {
		m.log.Error("could not fetch keys space metrics", zap.Error(err))
		return
	}

	// cleanup metrics store
	for i := range keys {
		if err := m.store.Del(keys[i]); err != nil {
			cid := hex.EncodeToString(keys[i])
			m.log.Error("could not remove key",
				zap.String("cid", cid),
				zap.Error(err))
		}
	}

	buf := make([]byte, 8)

	for cid := range items {
		binary.BigEndian.PutUint64(buf, items[cid])

		if err := m.store.Set(cid.Bytes(), buf); err != nil {
			m.log.Error("could not store space value",
				zap.Stringer("cid", cid),
				zap.Error(err))
		}
	}
}

func (m *syncStore) Update(cid refs.CID, size uint64, op SpaceOp) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch op {
	case RemSpace:
		if m.items[cid] < size {
			m.log.Error("space could not be negative")
			return
		}

		m.items[cid] -= size
	case AddSpace:
		m.items[cid] += size
	default:
		m.log.Error("unknown space operation", zap.Int("op", int(op)))
		return
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, m.items[cid])

	if err := m.store.Set(cid.Bytes(), buf); err != nil {
		m.log.Error("could not update space size", zap.Int("op", int(op)))
	}
}
