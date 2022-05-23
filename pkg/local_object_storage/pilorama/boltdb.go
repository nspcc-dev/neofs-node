package pilorama

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/nspcc-dev/neo-go/pkg/io"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.etcd.io/bbolt"
)

type boltForest struct {
	path string
	db   *bbolt.DB
}

var (
	dataBucket = []byte{0}
	logBucket  = []byte{1}
)

// NewBoltForest returns storage wrapper for storing operations on CRDT trees.
//
// Each tree is stored in a separate bucket by `CID + treeID` key.
// All integers are stored in little-endian unless explicitly specified otherwise.
//
// DB schema (for a single tree):
// timestamp is 8-byte, id is 4-byte.
//
// log storage (logBucket):
// timestamp in big-endian -> log operation
//
// tree storage (dataBucket):
// 't' + node (id) -> timestamp when the node first appeared
// 'p' + node (id) -> parent (id)
// 'm' + node (id) -> serialized meta
// 'c' + parent (id) + child (id) -> 0/1
func NewBoltForest(path string) ForestStorage {
	return &boltForest{path: path}
}

func (t *boltForest) Init() error { return nil }
func (t *boltForest) Open() error {
	if err := os.MkdirAll(filepath.Dir(t.path), os.ModePerm); err != nil {
		return err
	}

	db, err := bbolt.Open(t.path, os.ModePerm, bbolt.DefaultOptions)
	if err != nil {
		return err
	}

	t.db = db

	return db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(dataBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(logBucket)
		if err != nil {
			return err
		}
		return nil
	})
}
func (t *boltForest) Close() error { return t.db.Close() }

// TreeMove implements the Forest interface.
func (t *boltForest) TreeMove(cid cidSDK.ID, treeID string, m *Move) (*LogMove, error) {
	var lm *LogMove
	return lm, t.db.Update(func(tx *bbolt.Tx) error {
		bLog, bTree, err := t.getTreeBuckets(tx, cid, treeID)
		if err != nil {
			return err
		}

		m.Time = t.getLatestTimestamp(bLog)
		if m.Child == RootID {
			m.Child = t.findSpareID(bTree)
		}
		lm, err = t.applyOperation(bLog, bTree, m)
		return err
	})
}

// TreeAddByPath implements the Forest interface.
func (t *boltForest) TreeAddByPath(cid cidSDK.ID, treeID string, attr string, path []string, meta []KeyValue) ([]LogMove, error) {
	var lm []LogMove
	var key [17]byte

	err := t.db.Update(func(tx *bbolt.Tx) error {
		bLog, bTree, err := t.getTreeBuckets(tx, cid, treeID)
		if err != nil {
			return err
		}

		i, node, err := t.getPathPrefix(bTree, attr, path)
		if err != nil {
			return err
		}

		lm = make([]LogMove, len(path)-i+1)
		for j := i; j < len(path); j++ {
			lm[j-i].Move = Move{
				Parent: node,
				Meta: Meta{
					Time:  t.getLatestTimestamp(bLog),
					Items: []KeyValue{{Key: attr, Value: []byte(path[j])}},
				},
				Child: t.findSpareID(bTree),
			}

			err := t.do(bLog, bTree, key[:], &lm[j-i])
			if err != nil {
				return err
			}

			node = lm[j-i].Child
		}

		lm[len(lm)-1].Move = Move{
			Parent: node,
			Meta: Meta{
				Time:  t.getLatestTimestamp(bLog),
				Items: meta,
			},
			Child: t.findSpareID(bTree),
		}
		return t.do(bLog, bTree, key[:], &lm[len(lm)-1])
	})
	return lm, err
}

// getLatestTimestamp returns timestamp for a new operation which is guaranteed to be bigger than
// all timestamps corresponding to already stored operations.
// FIXME timestamp should be based on a node position in the container.
func (t *boltForest) getLatestTimestamp(bLog *bbolt.Bucket) uint64 {
	c := bLog.Cursor()
	key, _ := c.Last()
	if len(key) == 0 {
		return 1
	}
	return binary.BigEndian.Uint64(key) + 1
}

// findSpareID returns random unused ID.
func (t *boltForest) findSpareID(bTree *bbolt.Bucket) uint64 {
	id := uint64(rand.Int63())

	var key [9]byte
	key[0] = 't'
	binary.LittleEndian.PutUint64(key[1:], id)

	for {
		if bTree.Get(key[:]) == nil {
			return id
		}
		id = uint64(rand.Int63())
		binary.LittleEndian.PutUint64(key[1:], id)
	}
}

// TreeApply implements the Forest interface.
func (t *boltForest) TreeApply(cid cidSDK.ID, treeID string, m *Move) error {
	return t.db.Update(func(tx *bbolt.Tx) error {
		bLog, bTree, err := t.getTreeBuckets(tx, cid, treeID)
		if err != nil {
			return err
		}
		_, err = t.applyOperation(bLog, bTree, m)
		return err
	})
}

func (t *boltForest) getTreeBuckets(tx *bbolt.Tx, cid cidSDK.ID, treeID string) (*bbolt.Bucket, *bbolt.Bucket, error) {
	treeRoot := bucketName(cid, treeID)
	child, err := tx.CreateBucket(treeRoot)
	if err != nil && err != bbolt.ErrBucketExists {
		return nil, nil, err
	}

	var bLog, bData *bbolt.Bucket
	if err == nil {
		if bLog, err = child.CreateBucket(logBucket); err != nil {
			return nil, nil, err
		}
		if bData, err = child.CreateBucket(dataBucket); err != nil {
			return nil, nil, err
		}
	} else {
		child = tx.Bucket(treeRoot)
		bLog = child.Bucket(logBucket)
		bData = child.Bucket(dataBucket)
	}

	return bLog, bData, nil
}

func (t *boltForest) applyOperation(logBucket, treeBucket *bbolt.Bucket, m *Move) (*LogMove, error) {
	var lm LogMove
	var tmp LogMove
	var cKey [17]byte

	c := logBucket.Cursor()

	key, value := c.Last()

	// 1. Undo up until the desired timestamp is here.
	for len(key) == 8 && binary.BigEndian.Uint64(key) > m.Time {
		if err := t.logFromBytes(&tmp, key, value); err != nil {
			return nil, err
		}
		if err := t.undo(&tmp.Move, &tmp, treeBucket, cKey[:]); err != nil {
			return nil, err
		}
		key, value = c.Prev()
	}

	// 2. Insert the operation.
	if len(key) != 8 || binary.BigEndian.Uint64(key) != m.Time {
		lm.Move = *m
		if err := t.do(logBucket, treeBucket, cKey[:], &lm); err != nil {
			return nil, err
		}
	}
	key, value = c.Next()

	// 3. Re-apply all other operations.
	for len(key) == 8 {
		if err := t.logFromBytes(&tmp, key, value); err != nil {
			return nil, err
		}
		if err := t.do(logBucket, treeBucket, cKey[:], &tmp); err != nil {
			return nil, err
		}
		key, value = c.Next()
	}

	return &lm, nil
}

func (t *boltForest) do(lb *bbolt.Bucket, b *bbolt.Bucket, key []byte, op *LogMove) error {
	shouldPut := !t.isAncestor(b, key, op.Child, op.Parent) &&
		!(op.Parent != 0 && op.Parent != TrashID && b.Get(timestampKey(key, op.Parent)) == nil)
	shouldRemove := op.Parent == TrashID

	currParent := b.Get(parentKey(key, op.Child))
	if currParent != nil { // node is already in tree
		op.HasOld = true
		op.Old.Parent = binary.LittleEndian.Uint64(currParent)
		if err := op.Old.Meta.FromBytes(b.Get(metaKey(key, op.Child))); err != nil {
			return err
		}
	}

	binary.BigEndian.PutUint64(key, op.Time)
	if err := lb.Put(key[:8], t.logToBytes(op)); err != nil {
		return err
	}

	if !shouldPut {
		return nil
	}

	if shouldRemove {
		if currParent != nil {
			p := binary.LittleEndian.Uint64(currParent)
			if err := b.Delete(childrenKey(key, op.Child, p)); err != nil {
				return err
			}
		}
		return t.removeNode(b, key, op.Child)
	}

	if currParent == nil {
		if err := b.Put(timestampKey(key, op.Child), toUint64(op.Time)); err != nil {
			return err
		}
	} else {
		if err := b.Delete(childrenKey(key, op.Child, binary.LittleEndian.Uint64(currParent))); err != nil {
			return err
		}
	}
	return t.addNode(b, key, op.Child, op.Parent, op.Meta)
}

// removeNode removes node keys from the tree except the children key or its parent.
func (t *boltForest) removeNode(b *bbolt.Bucket, key []byte, node Node) error {
	if err := b.Delete(parentKey(key, node)); err != nil {
		return err
	}
	if err := b.Delete(metaKey(key, node)); err != nil {
		return err
	}
	return b.Delete(timestampKey(key, node))
}

// addNode adds node keys to the tree except the timestamp key.
func (t *boltForest) addNode(b *bbolt.Bucket, key []byte, child, parent Node, meta Meta) error {
	err := b.Put(parentKey(key, child), toUint64(parent))
	if err != nil {
		return err
	}
	err = b.Put(childrenKey(key, child, parent), []byte{1})
	if err != nil {
		return err
	}
	return b.Put(metaKey(key, child), meta.Bytes())
}

func (t *boltForest) undo(m *Move, lm *LogMove, b *bbolt.Bucket, key []byte) error {
	if err := b.Delete(childrenKey(key, m.Child, m.Parent)); err != nil {
		return err
	}

	if !lm.HasOld {
		return t.removeNode(b, key, m.Child)
	}
	return t.addNode(b, key, m.Child, lm.Old.Parent, lm.Old.Meta)
}

func (t *boltForest) isAncestor(b *bbolt.Bucket, key []byte, parent, child Node) bool {
	key[0] = 'p'
	for c := child; c != parent; {
		binary.LittleEndian.PutUint64(key[1:], c)
		rawParent := b.Get(key[:9])
		if len(rawParent) != 8 {
			return false
		}
		c = binary.LittleEndian.Uint64(rawParent)
	}
	return true
}

// TreeGetByPath implements the Forest interface.
func (t *boltForest) TreeGetByPath(cid cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]Node, error) {
	if len(path) == 0 {
		return nil, nil
	}

	var nodes []Node

	return nodes, t.db.View(func(tx *bbolt.Tx) error {
		treeRoot := tx.Bucket(bucketName(cid, treeID))
		if treeRoot == nil {
			return ErrTreeNotFound
		}

		b := treeRoot.Bucket(dataBucket)

		i, curNode, err := t.getPathPrefix(b, attr, path[:len(path)-1])
		if err != nil {
			return err
		}
		if i < len(path)-1 {
			return nil
		}

		c := b.Cursor()

		var (
			metaKey      [9]byte
			id           [9]byte
			childID      [9]byte
			m            Meta
			maxTimestamp uint64
		)

		id[0] = 'c'
		metaKey[0] = 'm'

		binary.LittleEndian.PutUint64(id[1:], curNode)

		key, _ := c.Seek(id[:])
		for len(key) == 1+8+8 && bytes.Equal(id[:9], key[:9]) {
			child := binary.LittleEndian.Uint64(key[9:])
			copy(metaKey[1:], key[9:17])

			if m.FromBytes(b.Get(metaKey[:])) == nil && string(m.GetAttr(attr)) == path[len(path)-1] {
				if latest {
					ts := binary.LittleEndian.Uint64(b.Get(timestampKey(childID[:], child)))
					if ts >= maxTimestamp {
						nodes = append(nodes[:0], child)
						maxTimestamp = ts
					}
				} else {
					nodes = append(nodes, child)
				}
			}
			key, _ = c.Next()
		}

		return nil
	})
}

// TreeGetMeta implements the forest interface.
func (t *boltForest) TreeGetMeta(cid cidSDK.ID, treeID string, nodeID Node) (Meta, Node, error) {
	key := parentKey(make([]byte, 9), nodeID)

	var m Meta
	var parentID uint64

	err := t.db.View(func(tx *bbolt.Tx) error {
		treeRoot := tx.Bucket(bucketName(cid, treeID))
		if treeRoot == nil {
			return ErrTreeNotFound
		}

		b := treeRoot.Bucket(dataBucket)
		if data := b.Get(key); len(data) == 8 {
			parentID = binary.LittleEndian.Uint64(data)
		}
		return m.FromBytes(b.Get(metaKey(key, nodeID)))
	})

	return m, parentID, err
}

// TreeGetChildren implements the Forest interface.
func (t *boltForest) TreeGetChildren(cid cidSDK.ID, treeID string, nodeID Node) ([]uint64, error) {
	key := make([]byte, 9)
	key[0] = 'c'
	binary.LittleEndian.PutUint64(key[1:], nodeID)

	var children []uint64

	err := t.db.View(func(tx *bbolt.Tx) error {
		treeRoot := tx.Bucket(bucketName(cid, treeID))
		if treeRoot == nil {
			return ErrTreeNotFound
		}

		b := treeRoot.Bucket(dataBucket)
		c := b.Cursor()
		for k, _ := c.Seek(key); len(k) == 17 && binary.LittleEndian.Uint64(k[1:]) == nodeID; k, _ = c.Next() {
			children = append(children, binary.LittleEndian.Uint64(k[9:]))
		}
		return nil
	})

	return children, err
}

func (t *boltForest) getPathPrefix(bTree *bbolt.Bucket, attr string, path []string) (int, Node, error) {
	var key [9]byte

	c := bTree.Cursor()

	var curNode Node
	var m Meta

loop:
	for i := range path {
		key[0] = 'c'
		binary.LittleEndian.PutUint64(key[1:], curNode)

		childKey, _ := c.Seek(key[:])
		for {
			if len(childKey) != 17 || binary.LittleEndian.Uint64(childKey[1:]) != curNode {
				break
			}

			child := binary.LittleEndian.Uint64(childKey[9:])
			if err := m.FromBytes(bTree.Get(metaKey(key[:], child))); err != nil {
				return 0, 0, err
			}

			// Internal nodes have exactly one attribute.
			if len(m.Items) == 1 && m.Items[0].Key == attr && string(m.Items[0].Value) == path[i] {
				curNode = child
				continue loop
			}
			childKey, _ = c.Next()
		}
		return i, curNode, nil
	}

	return len(path), curNode, nil
}

func (t *boltForest) logFromBytes(lm *LogMove, key []byte, data []byte) error {
	r := io.NewBinReaderFromBuf(data)
	lm.Child = r.ReadU64LE()
	lm.Parent = r.ReadU64LE()
	if err := lm.Meta.FromBytes(r.ReadVarBytes()); err != nil {
		return err
	}

	lm.HasOld = r.ReadBool()
	if lm.HasOld {
		lm.Old.Parent = r.ReadU64LE()
		if err := lm.Old.Meta.FromBytes(r.ReadVarBytes()); err != nil {
			return err
		}
	}

	return r.Err
}

func (t *boltForest) logToBytes(lm *LogMove) []byte {
	w := io.NewBufBinWriter()
	w.WriteU64LE(lm.Child)
	w.WriteU64LE(lm.Parent)
	w.WriteVarBytes(lm.Meta.Bytes())
	w.WriteBool(lm.HasOld)
	if lm.HasOld {
		w.WriteU64LE(lm.Old.Parent)
		w.WriteVarBytes(lm.Old.Meta.Bytes())
	}
	return w.Bytes()
}

func bucketName(cid cidSDK.ID, treeID string) []byte {
	return []byte(cid.String() + treeID)
}

// 't' + node (id) -> timestamp when the node first appeared
func timestampKey(key []byte, child Node) []byte {
	key[0] = 't'
	binary.LittleEndian.PutUint64(key[1:], child)
	return key[:9]
}

// 'p' + node (id) -> parent (id)
func parentKey(key []byte, child Node) []byte {
	key[0] = 'p'
	binary.LittleEndian.PutUint64(key[1:], child)
	return key[:9]
}

// 'm' + node (id) -> serialized meta
func metaKey(key []byte, child Node) []byte {
	key[0] = 'm'
	binary.LittleEndian.PutUint64(key[1:], child)
	return key[:9]
}

// 'c' + parent (id) + child (id) -> 0/1
func childrenKey(key []byte, child, parent Node) []byte {
	key[0] = 'c'
	binary.LittleEndian.PutUint64(key[1:], parent)
	binary.LittleEndian.PutUint64(key[9:], child)
	return key[:17]
}

func toUint64(x uint64) []byte {
	var a [8]byte
	binary.LittleEndian.PutUint64(a[:], x)
	return a[:]
}
