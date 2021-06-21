package meta

import (
	"encoding/binary"
	"io"
	"strings"

	"github.com/cockroachdb/pebble"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

func (db *DB) Containers() (list []*cid.ID, err error) {
	return db.containers()
}

func (db *DB) containers() ([]*cid.ID, error) {
	result := make([]*cid.ID, 0)

	key := []byte{primaryPrefix}
	iter := db.newPrefixIterator(key)
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.SeekGE(key) {
		parts := splitKey(iter.Key())

		var cidV2 refs.ContainerID
		cidV2.SetValue(cloneBytes(parts[0]))
		result = append(result, cid.NewFromV2(&cidV2))

		key = nextKey(appendKey(key[:1], parts[0]))
	}

	return result, nil
}

func (db *DB) ContainerSize(id *cid.ID) (size uint64, err error) {
	return db.containerSize(id)
}

func (db *DB) containerSize(id *cid.ID) (uint64, error) {
	key := cidBucketKey(id, containerVolumePrefix, nil)
	sz, c, err := db.db.Get(key)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	return parseContainerSize(sz), nil
}

func parseContainerID(name []byte) (*cid.ID, error) {
	strName := string(name)

	if strings.Contains(strName, invalidBase58String) {
		return nil, nil
	}

	id := cid.New()

	return id, id.Parse(strName)
}

func parseContainerSize(v []byte) uint64 {
	if len(v) == 0 {
		return 0
	}

	return binary.LittleEndian.Uint64(v)
}

func (db *DB) changeContainerSize(tx *pebble.Batch, id *cid.ID, delta uint64, increase bool) error {
	if !increase {
		delta = uint64(-int64(delta))
	}

	buf := make([]byte, 8) // consider using sync.Pool to decrease allocations
	binary.LittleEndian.PutUint64(buf, delta)

	key := cidBucketKey(id, containerVolumePrefix, nil)
	return tx.Merge(key, buf, nil)
}

var valueMerger = &pebble.Merger{
	Merge: merger,
	Name:  "neofs.addUint64",
}

func merger(_, v []byte) (pebble.ValueMerger, error) {
	return &mergeAdd{
		size: int64(binary.LittleEndian.Uint64(v)),
	}, nil
}

type mergeAdd struct {
	size int64
	buf  [8]byte
}

func (m *mergeAdd) MergeNewer(value []byte) error {
	m.size += int64(binary.LittleEndian.Uint64(value))
	return nil
}

func (m *mergeAdd) MergeOlder(value []byte) error {
	m.size += int64(binary.LittleEndian.Uint64(value))
	return nil
}

func (m *mergeAdd) Finish(includesBase bool) ([]byte, io.Closer, error) {
	binary.LittleEndian.PutUint64(m.buf[:], uint64(m.size))
	return m.buf[:], nil, nil
}
