package pilorama

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.etcd.io/bbolt"
)

type boltForest struct {
	db *bbolt.DB

	modeMtx sync.Mutex
	mode    mode.Mode
	cfg
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
// - 't' + node (id) -> timestamp when the node first appeared,
// - 'p' + node (id) -> parent (id),
// - 'm' + node (id) -> serialized meta,
// - 'c' + parent (id) + child (id) -> 0/1,
// - 'i' + 0 + attrKey + 0 + attrValue + 0 + parent (id) + node (id) -> 0/1 (1 for automatically created nodes).
func NewBoltForest(opts ...Option) ForestStorage {
	b := boltForest{
		cfg: cfg{
			perm:          os.ModePerm,
			maxBatchDelay: bbolt.DefaultMaxBatchDelay,
			maxBatchSize:  bbolt.DefaultMaxBatchSize,
		},
	}

	for i := range opts {
		opts[i](&b.cfg)
	}

	return &b
}

func (t *boltForest) SetMode(m mode.Mode) error {
	t.modeMtx.Lock()
	defer t.modeMtx.Unlock()

	if t.mode == m {
		return nil
	}
	if t.mode.ReadOnly() == m.ReadOnly() {
		return nil
	}

	err := t.Close()
	if err == nil {
		if err = t.Open(m.ReadOnly()); err == nil {
			err = t.Init()
		}
	}
	if err != nil {
		return fmt.Errorf("can't set pilorama mode (old=%s, new=%s): %w", t.mode, m, err)
	}

	t.mode = m
	return nil
}
func (t *boltForest) Open(readOnly bool) error {
	err := util.MkdirAllX(filepath.Dir(t.path), t.perm)
	if err != nil {
		return fmt.Errorf("can't create dir %s for the pilorama: %w", t.path, err)
	}

	opts := *bbolt.DefaultOptions
	opts.ReadOnly = readOnly
	opts.NoSync = t.noSync
	opts.Timeout = 100 * time.Millisecond

	t.db, err = bbolt.Open(t.path, t.perm, &opts)
	if err != nil {
		return fmt.Errorf("can't open the pilorama DB: %w", err)
	}

	t.db.MaxBatchSize = t.maxBatchSize
	t.db.MaxBatchDelay = t.maxBatchDelay

	return nil
}
func (t *boltForest) Init() error {
	if t.db.IsReadOnly() {
		return nil
	}
	return t.db.Update(func(tx *bbolt.Tx) error {
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
func (t *boltForest) Close() error {
	if t.db != nil {
		return t.db.Close()
	}
	return nil
}

// TreeMove implements the Forest interface.
func (t *boltForest) TreeMove(d CIDDescriptor, treeID string, m *Move) (*LogMove, error) {
	if !d.checkValid() {
		return nil, ErrInvalidCIDDescriptor
	}

	var lm LogMove
	lm.Move = *m
	return &lm, t.db.Batch(func(tx *bbolt.Tx) error {
		bLog, bTree, err := t.getTreeBuckets(tx, d.CID, treeID)
		if err != nil {
			return err
		}

		lm.Time = t.getLatestTimestamp(bLog, d.Position, d.Size)
		if lm.Child == RootID {
			lm.Child = t.findSpareID(bTree)
		}
		return t.do(bLog, bTree, make([]byte, 17), &lm)
	})
}

// TreeExists implements the Forest interface.
func (t *boltForest) TreeExists(cid cidSDK.ID, treeID string) (bool, error) {
	var exists bool

	err := t.db.View(func(tx *bbolt.Tx) error {
		treeRoot := tx.Bucket(bucketName(cid, treeID))
		exists = treeRoot != nil
		return nil
	})

	return exists, err
}

// TreeAddByPath implements the Forest interface.
func (t *boltForest) TreeAddByPath(d CIDDescriptor, treeID string, attr string, path []string, meta []KeyValue) ([]LogMove, error) {
	if !d.checkValid() {
		return nil, ErrInvalidCIDDescriptor
	}
	if !isAttributeInternal(attr) {
		return nil, ErrNotPathAttribute
	}

	var lm []LogMove
	var key [17]byte

	err := t.db.Batch(func(tx *bbolt.Tx) error {
		bLog, bTree, err := t.getTreeBuckets(tx, d.CID, treeID)
		if err != nil {
			return err
		}

		i, node, err := t.getPathPrefix(bTree, attr, path)
		if err != nil {
			return err
		}

		ts := t.getLatestTimestamp(bLog, d.Position, d.Size)
		lm = make([]LogMove, len(path)-i+1)
		for j := i; j < len(path); j++ {
			lm[j-i].Move = Move{
				Parent: node,
				Meta: Meta{
					Time:  ts,
					Items: []KeyValue{{Key: attr, Value: []byte(path[j])}},
				},
				Child: t.findSpareID(bTree),
			}

			err := t.do(bLog, bTree, key[:], &lm[j-i])
			if err != nil {
				return err
			}

			ts = nextTimestamp(ts, uint64(d.Position), uint64(d.Size))
			node = lm[j-i].Child
		}

		lm[len(lm)-1].Move = Move{
			Parent: node,
			Meta: Meta{
				Time:  ts,
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
func (t *boltForest) getLatestTimestamp(bLog *bbolt.Bucket, pos, size int) uint64 {
	var ts uint64

	c := bLog.Cursor()
	key, _ := c.Last()
	if len(key) != 0 {
		ts = binary.BigEndian.Uint64(key)
	}
	return nextTimestamp(ts, uint64(pos), uint64(size))
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
func (t *boltForest) TreeApply(d CIDDescriptor, treeID string, m *Move, backgroundSync bool) error {
	if !d.checkValid() {
		return ErrInvalidCIDDescriptor
	}

	if backgroundSync {
		var seen bool
		err := t.db.View(func(tx *bbolt.Tx) error {
			treeRoot := tx.Bucket(bucketName(d.CID, treeID))
			if treeRoot == nil {
				return nil
			}

			b := treeRoot.Bucket(logBucket)

			var logKey [8]byte
			binary.BigEndian.PutUint64(logKey[:], m.Time)
			seen = b.Get(logKey[:]) != nil
			return nil
		})
		if err != nil || seen {
			return err
		}
	}

	return t.db.Batch(func(tx *bbolt.Tx) error {
		bLog, bTree, err := t.getTreeBuckets(tx, d.CID, treeID)
		if err != nil {
			return err
		}

		lm := &LogMove{Move: *m}
		return t.applyOperation(bLog, bTree, lm)
	})
}

func (t *boltForest) getTreeBuckets(tx *bbolt.Tx, cid cidSDK.ID, treeID string) (*bbolt.Bucket, *bbolt.Bucket, error) {
	treeRoot := bucketName(cid, treeID)
	child := tx.Bucket(treeRoot)
	if child != nil {
		return child.Bucket(logBucket), child.Bucket(dataBucket), nil
	}

	child, err := tx.CreateBucket(treeRoot)
	if err != nil {
		return nil, nil, err
	}
	bLog, err := child.CreateBucket(logBucket)
	if err != nil {
		return nil, nil, err
	}
	bData, err := child.CreateBucket(dataBucket)
	if err != nil {
		return nil, nil, err
	}
	return bLog, bData, nil
}

func (t *boltForest) applyOperation(logBucket, treeBucket *bbolt.Bucket, lm *LogMove) error {
	var tmp LogMove
	var cKey [17]byte

	var logKey [8]byte
	binary.BigEndian.PutUint64(logKey[:], lm.Time)
	if logBucket.Get(logKey[:]) != nil {
		return nil
	}

	c := logBucket.Cursor()

	key, value := c.Last()

	b := bytes.NewReader(nil)
	r := io.NewBinReaderFromIO(b)

	// 1. Undo up until the desired timestamp is here.
	for len(key) == 8 && binary.BigEndian.Uint64(key) > lm.Time {
		b.Reset(value)
		if err := t.logFromBytes(&tmp, r); err != nil {
			return err
		}
		if err := t.undo(&tmp.Move, &tmp, treeBucket, cKey[:]); err != nil {
			return err
		}
		key, value = c.Prev()
	}

	key, _ = c.Next()

	// 2. Insert the operation.
	if len(key) != 8 || binary.BigEndian.Uint64(key) != lm.Time {
		if err := t.do(logBucket, treeBucket, cKey[:], lm); err != nil {
			return err
		}
	}

	if key == nil {
		// The operation is inserted in the beginning, reposition the cursor.
		// Otherwise, `Next` call will return currently inserted operation.
		c.First()
	}
	key, value = c.Seek(key)

	// 3. Re-apply all other operations.
	for len(key) == 8 {
		b.Reset(value)
		if err := t.logFromBytes(&tmp, r); err != nil {
			return err
		}
		if err := t.do(logBucket, treeBucket, cKey[:], &tmp); err != nil {
			return err
		}
		key, value = c.Next()
	}

	return nil
}

func (t *boltForest) do(lb *bbolt.Bucket, b *bbolt.Bucket, key []byte, op *LogMove) error {
	currParent := b.Get(parentKey(key, op.Child))
	op.HasOld = currParent != nil
	if currParent != nil { // node is already in tree
		op.Old.Parent = binary.LittleEndian.Uint64(currParent)
		if err := op.Old.Meta.FromBytes(b.Get(metaKey(key, op.Child))); err != nil {
			return err
		}
	} else {
		op.HasOld = false
		op.Old = nodeInfo{}
	}

	binary.BigEndian.PutUint64(key, op.Time)
	if err := lb.Put(key[:8], t.logToBytes(op)); err != nil {
		return err
	}

	if op.Child == op.Parent || t.isAncestor(b, op.Child, op.Parent) {
		return nil
	}

	if currParent == nil {
		if err := b.Put(timestampKey(key, op.Child), toUint64(op.Time)); err != nil {
			return err
		}
	} else {
		parent := binary.LittleEndian.Uint64(currParent)
		if err := b.Delete(childrenKey(key, op.Child, parent)); err != nil {
			return err
		}
		for i := range op.Old.Meta.Items {
			if isAttributeInternal(op.Old.Meta.Items[i].Key) {
				key = internalKey(key, op.Old.Meta.Items[i].Key, string(op.Old.Meta.Items[i].Value), parent, op.Child)
				err := b.Delete(key)
				if err != nil {
					return err
				}
			}
		}
	}
	return t.addNode(b, key, op.Child, op.Parent, op.Meta)
}

// removeNode removes node keys from the tree except the children key or its parent.
func (t *boltForest) removeNode(b *bbolt.Bucket, key []byte, node, parent Node) error {
	if err := b.Delete(parentKey(key, node)); err != nil {
		return err
	}
	var meta Meta
	var k = metaKey(key, node)
	if err := meta.FromBytes(b.Get(k)); err == nil {
		for i := range meta.Items {
			if isAttributeInternal(meta.Items[i].Key) {
				err := b.Delete(internalKey(nil, meta.Items[i].Key, string(meta.Items[i].Value), parent, node))
				if err != nil {
					return err
				}
			}
		}
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
	err = b.Put(metaKey(key, child), meta.Bytes())
	if err != nil {
		return err
	}

	for i := range meta.Items {
		if !isAttributeInternal(meta.Items[i].Key) {
			continue
		}

		key = internalKey(key, meta.Items[i].Key, string(meta.Items[i].Value), parent, child)
		if len(meta.Items) == 1 {
			err = b.Put(key, []byte{1})
		} else {
			err = b.Put(key, []byte{0})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *boltForest) undo(m *Move, lm *LogMove, b *bbolt.Bucket, key []byte) error {
	if err := b.Delete(childrenKey(key, m.Child, m.Parent)); err != nil {
		return err
	}

	if !lm.HasOld {
		return t.removeNode(b, key, m.Child, m.Parent)
	}
	return t.addNode(b, key, m.Child, lm.Old.Parent, lm.Old.Meta)
}

func (t *boltForest) isAncestor(b *bbolt.Bucket, parent, child Node) bool {
	key := make([]byte, 9)
	key[0] = 'p'
	for node := child; node != parent; {
		binary.LittleEndian.PutUint64(key[1:], node)
		rawParent := b.Get(key)
		if len(rawParent) != 8 {
			return false
		}
		node = binary.LittleEndian.Uint64(rawParent)
	}
	return true
}

// TreeGetByPath implements the Forest interface.
func (t *boltForest) TreeGetByPath(cid cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]Node, error) {
	if !isAttributeInternal(attr) {
		return nil, ErrNotPathAttribute
	}

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

		var (
			childID      [9]byte
			maxTimestamp uint64
		)

		c := b.Cursor()

		attrKey := internalKey(nil, attr, path[len(path)-1], curNode, 0)
		attrKey = attrKey[:len(attrKey)-8]
		childKey, _ := c.Seek(attrKey)
		for len(childKey) == len(attrKey)+8 && bytes.Equal(attrKey, childKey[:len(childKey)-8]) {
			child := binary.LittleEndian.Uint64(childKey[len(childKey)-8:])
			if latest {
				ts := binary.LittleEndian.Uint64(b.Get(timestampKey(childID[:], child)))
				if ts >= maxTimestamp {
					nodes = append(nodes[:0], child)
					maxTimestamp = ts
				}
			} else {
				nodes = append(nodes, child)
			}
			childKey, _ = c.Next()
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

// TreeList implements the Forest interface.
func (t *boltForest) TreeList(cid cidSDK.ID) ([]string, error) {
	var ids []string
	cidRaw := []byte(cid.EncodeToString())
	cidLen := len(cidRaw)

	err := t.db.View(func(tx *bbolt.Tx) error {
		c := tx.Cursor()
		for k, _ := c.Seek(cidRaw); k != nil; k, _ = c.Next() {
			if !bytes.HasPrefix(k, cidRaw) {
				return nil
			}

			ids = append(ids, string(k[cidLen:]))
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not list trees: %w", err)
	}

	return ids, nil
}

// TreeGetOpLog implements the pilorama.Forest interface.
func (t *boltForest) TreeGetOpLog(cid cidSDK.ID, treeID string, height uint64) (Move, error) {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, height)

	var lm Move

	err := t.db.View(func(tx *bbolt.Tx) error {
		treeRoot := tx.Bucket(bucketName(cid, treeID))
		if treeRoot == nil {
			return ErrTreeNotFound
		}

		c := treeRoot.Bucket(logBucket).Cursor()
		if _, data := c.Seek(key); data != nil {
			return t.moveFromBytes(&lm, data)
		}
		return nil
	})

	return lm, err
}

// TreeDrop implements the pilorama.Forest interface.
func (t *boltForest) TreeDrop(cid cidSDK.ID, treeID string) error {
	return t.db.Batch(func(tx *bbolt.Tx) error {
		if treeID == "" {
			c := tx.Cursor()
			prefix := []byte(cid.EncodeToString())
			for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
				err := tx.DeleteBucket(k)
				if err != nil {
					return err
				}
			}
			return nil
		}
		err := tx.DeleteBucket(bucketName(cid, treeID))
		if errors.Is(err, bbolt.ErrBucketNotFound) {
			return ErrTreeNotFound
		}
		return err
	})
}

func (t *boltForest) getPathPrefix(bTree *bbolt.Bucket, attr string, path []string) (int, Node, error) {
	c := bTree.Cursor()

	var curNode Node
	var attrKey []byte

loop:
	for i := range path {
		attrKey = internalKey(attrKey, attr, path[i], curNode, 0)
		attrKey = attrKey[:len(attrKey)-8]

		childKey, value := c.Seek(attrKey)
		for len(childKey) == len(attrKey)+8 && bytes.Equal(attrKey, childKey[:len(childKey)-8]) {
			if len(value) == 1 && value[0] == 1 {
				curNode = binary.LittleEndian.Uint64(childKey[len(childKey)-8:])
				continue loop
			}
			childKey, value = c.Next()
		}

		return i, curNode, nil
	}

	return len(path), curNode, nil
}

func (t *boltForest) moveFromBytes(m *Move, data []byte) error {
	r := io.NewBinReaderFromBuf(data)
	m.Child = r.ReadU64LE()
	m.Parent = r.ReadU64LE()
	m.Meta.DecodeBinary(r)
	return r.Err
}

func (t *boltForest) logFromBytes(lm *LogMove, r *io.BinReader) error {
	lm.Child = r.ReadU64LE()
	lm.Parent = r.ReadU64LE()
	lm.Meta.DecodeBinary(r)
	lm.HasOld = r.ReadBool()
	if lm.HasOld {
		lm.Old.Parent = r.ReadU64LE()
		lm.Old.Meta.DecodeBinary(r)
	}
	return r.Err
}

func (t *boltForest) logToBytes(lm *LogMove) []byte {
	w := io.NewBufBinWriter()
	size := 8 + 8 + lm.Meta.Size() + 1
	if lm.HasOld {
		size += 8 + lm.Old.Meta.Size()
	}

	w.Grow(size)
	w.WriteU64LE(lm.Child)
	w.WriteU64LE(lm.Parent)
	lm.Meta.EncodeBinary(w.BinWriter)
	w.WriteBool(lm.HasOld)
	if lm.HasOld {
		w.WriteU64LE(lm.Old.Parent)
		lm.Old.Meta.EncodeBinary(w.BinWriter)
	}
	return w.Bytes()
}

func bucketName(cid cidSDK.ID, treeID string) []byte {
	return []byte(cid.String() + treeID)
}

// 't' + node (id) -> timestamp when the node first appeared.
func timestampKey(key []byte, child Node) []byte {
	key[0] = 't'
	binary.LittleEndian.PutUint64(key[1:], child)
	return key[:9]
}

// 'p' + node (id) -> parent (id).
func parentKey(key []byte, child Node) []byte {
	key[0] = 'p'
	binary.LittleEndian.PutUint64(key[1:], child)
	return key[:9]
}

// 'm' + node (id) -> serialized meta.
func metaKey(key []byte, child Node) []byte {
	key[0] = 'm'
	binary.LittleEndian.PutUint64(key[1:], child)
	return key[:9]
}

// 'c' + parent (id) + child (id) -> 0/1.
func childrenKey(key []byte, child, parent Node) []byte {
	key[0] = 'c'
	binary.LittleEndian.PutUint64(key[1:], parent)
	binary.LittleEndian.PutUint64(key[9:], child)
	return key[:17]
}

// 'i' + attribute name (string) + attribute value (string) + parent (id) + node (id) -> 0/1.
func internalKey(key []byte, k, v string, parent, node Node) []byte {
	size := 1 /* prefix */ + 2*2 /* len */ + 2*8 /* nodes */ + len(k) + len(v)
	if cap(key) < size {
		key = make([]byte, 0, size)
	}

	key = key[:0]
	key = append(key, 'i')

	l := len(k)
	key = append(key, byte(l), byte(l>>8))
	key = append(key, k...)

	l = len(v)
	key = append(key, byte(l), byte(l>>8))
	key = append(key, v...)

	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], parent)
	key = append(key, raw[:]...)

	binary.LittleEndian.PutUint64(raw[:], node)
	key = append(key, raw[:]...)
	return key
}

func toUint64(x uint64) []byte {
	var a [8]byte
	binary.LittleEndian.PutUint64(a[:], x)
	return a[:]
}
