package meta

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"sync"

	"github.com/nspcc-dev/neo-go/pkg/core/mpt"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type containerStorage struct {
	m        sync.RWMutex
	opsBatch map[string][]byte

	path string
	mpt  *mpt.Trie
	db   storage.Store
}

func (s *containerStorage) drop() error {
	s.m.Lock()
	defer s.m.Unlock()

	err := s.db.Close()
	if err != nil {
		return fmt.Errorf("close container storage: %w", err)
	}

	err = os.RemoveAll(s.path)
	if err != nil {
		return fmt.Errorf("remove container storage: %w", err)
	}

	return nil
}

func (s *containerStorage) putObject(e objEvent) error {
	s.m.Lock()
	defer s.m.Unlock()

	newKVs := make(map[string][]byte)
	commsuffix := e.oID[:]

	// batching that is implemented for MPT ignores key's first byte

	newKVs[string(append([]byte{0, oidIndex}, commsuffix...))] = []byte{}
	newKVs[string(append([]byte{0, sizeIndex}, commsuffix...))] = e.size.Bytes()
	if len(e.firstObject) > 0 {
		newKVs[string(append([]byte{0, firstPartIndex}, commsuffix...))] = e.firstObject
	}
	if len(e.prevObject) > 0 {
		newKVs[string(append([]byte{0, previousPartIndex}, commsuffix...))] = e.prevObject
	}
	if len(e.deletedObjects) > 0 {
		newKVs[string(append([]byte{0, deletedIndex}, commsuffix...))] = e.deletedObjects
		maps.Copy(newKVs, deleteObjectsOps(s.db, e.deletedObjects))
	}
	if len(e.lockedObjects) > 0 {
		newKVs[string(append([]byte{0, lockedIndex}, commsuffix...))] = e.lockedObjects
	}
	if e.typ != objectsdk.TypeRegular {
		newKVs[string(append([]byte{0, typeIndex}, commsuffix...))] = []byte{byte(e.typ)}
	}

	if s.opsBatch == nil {
		s.opsBatch = make(map[string][]byte)
	}
	maps.Copy(s.opsBatch, newKVs)

	err := s.db.PutChangeSet(mptToStoreBatch(newKVs), nil)
	if err != nil {
		return fmt.Errorf("put MPT KVs to the raw storage manually: %w", err)
	}

	// TODO: receive full object header and store its non-MPT parts in
	//  persistent object storage

	return nil
}

func deleteObjectsOps(s storage.Store, objects []byte) map[string][]byte {
	resMpt := make(map[string][]byte)
	rng := storage.SeekRange{}

	for len(objects) > 0 {
		o := objects[:oid.Size]
		objects = objects[oid.Size:]
		rng.Start = append([]byte{oidIndex}, o...)
		stopKey := lastObjectKey(o)

		s.Seek(rng, func(k, v []byte) bool {
			if len(k) >= 1+oid.Size && bytes.HasPrefix(k[1:], o) {
				resMpt[string(append([]byte{0}, k...))] = nil // nil means "delete"
				return true
			}

			return bytes.Compare(k, stopKey) < 0
		})
	}

	return resMpt
}

// lastObjectKey returns the least possible key in sorted DB list that
// proves there will not be information about the object anymore.
func lastObjectKey(rawOID []byte) []byte {
	res := make([]byte, 0, len(rawOID)+1)
	res = append(res, lastEnumIndex-1)

	return append(res, rawOID...)
}

// mptToStoreBatch drops the first byte from every key in the map.
func mptToStoreBatch(b map[string][]byte) map[string][]byte {
	res := make(map[string][]byte, len(b))
	for k, v := range b {
		res[k[1:]] = v
	}

	return res
}

func storageForContainer(rootPath string, cID cid.ID) (*containerStorage, error) {
	p := path.Join(rootPath, cID.EncodeToString())

	st, err := storage.NewBoltDBStore(dbconfig.BoltDBOptions{FilePath: p, ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("open bolt store at %q: %w", p, err)
	}

	var prevRootNode mpt.Node
	root, err := st.Get([]byte{rootKey})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		if err != nil {
			return nil, fmt.Errorf("get state root from db: %w", err)
		}

		if len(root) != util.Uint256Size {
			return nil, fmt.Errorf("root hash from db is %d bytes long, expect %d", len(root), util.Uint256Size)
		}

		prevRootNode = mpt.NewHashNode([util.Uint256Size]byte(root))
	}

	return &containerStorage{
		path: p,
		mpt:  mpt.NewTrie(prevRootNode, mpt.ModeLatest, storage.NewMemCachedStore(st)),
		db:   st,
	}, nil
}
