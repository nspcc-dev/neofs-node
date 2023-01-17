package pilorama

import (
	"sort"
	"strings"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// memoryForest represents multiple replicating trees sharing a single storage.
type memoryForest struct {
	// treeMap maps tree identifier (container ID + name) to the replicated log.
	treeMap map[string]*state
}

var _ Forest = (*memoryForest)(nil)

// NewMemoryForest creates new empty forest.
// TODO: this function will eventually be removed and is here for debugging.
func NewMemoryForest() ForestStorage {
	return &memoryForest{
		treeMap: make(map[string]*state),
	}
}

// TreeMove implements the Forest interface.
func (f *memoryForest) TreeMove(d CIDDescriptor, treeID string, op *Move) (*LogMove, error) {
	if !d.checkValid() {
		return nil, ErrInvalidCIDDescriptor
	}

	fullID := d.CID.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		s = newState()
		f.treeMap[fullID] = s
	}

	op.Time = s.timestamp(d.Position, d.Size)
	if op.Child == RootID {
		op.Child = s.findSpareID()
	}

	lm := s.do(op)
	s.operations = append(s.operations, lm)
	return &lm.Move, nil
}

// TreeAddByPath implements the Forest interface.
func (f *memoryForest) TreeAddByPath(d CIDDescriptor, treeID string, attr string, path []string, m []KeyValue) ([]LogMove, error) {
	if !d.checkValid() {
		return nil, ErrInvalidCIDDescriptor
	}
	if !isAttributeInternal(attr) {
		return nil, ErrNotPathAttribute
	}

	fullID := d.CID.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		s = newState()
		f.treeMap[fullID] = s
	}

	i, node := s.getPathPrefix(attr, path)
	lm := make([]LogMove, len(path)-i+1)
	for j := i; j < len(path); j++ {
		op := s.do(&Move{
			Parent: node,
			Meta: Meta{
				Time:  s.timestamp(d.Position, d.Size),
				Items: []KeyValue{{Key: attr, Value: []byte(path[j])}}},
			Child: s.findSpareID(),
		})
		lm[j-i] = op.Move
		node = op.Child
		s.operations = append(s.operations, op)
	}

	mCopy := make([]KeyValue, len(m))
	copy(mCopy, m)
	op := s.do(&Move{
		Parent: node,
		Meta: Meta{
			Time:  s.timestamp(d.Position, d.Size),
			Items: mCopy,
		},
		Child: s.findSpareID(),
	})
	lm[len(lm)-1] = op.Move
	return lm, nil
}

// TreeApply implements the Forest interface.
func (f *memoryForest) TreeApply(d CIDDescriptor, treeID string, op *Move, _ bool) error {
	if !d.checkValid() {
		return ErrInvalidCIDDescriptor
	}

	fullID := d.CID.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		s = newState()
		f.treeMap[fullID] = s
	}

	return s.Apply(op)
}

func (f *memoryForest) Init() error {
	return nil
}

func (f *memoryForest) Open(bool) error {
	return nil
}
func (f *memoryForest) SetMode(mode.Mode) error {
	return nil
}
func (f *memoryForest) Close() error {
	return nil
}

// TreeGetByPath implements the Forest interface.
func (f *memoryForest) TreeGetByPath(cid cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]Node, error) {
	if !isAttributeInternal(attr) {
		return nil, ErrNotPathAttribute
	}

	fullID := cid.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		return nil, ErrTreeNotFound
	}

	return s.get(attr, path, latest), nil
}

// TreeGetMeta implements the Forest interface.
func (f *memoryForest) TreeGetMeta(cid cidSDK.ID, treeID string, nodeID Node) (Meta, Node, error) {
	fullID := cid.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		return Meta{}, 0, ErrTreeNotFound
	}

	return s.getMeta(nodeID), s.infoMap[nodeID].Parent, nil
}

// TreeGetChildren implements the Forest interface.
func (f *memoryForest) TreeGetChildren(cid cidSDK.ID, treeID string, nodeID Node) ([]uint64, error) {
	fullID := cid.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		return nil, ErrTreeNotFound
	}

	children, ok := s.childMap[nodeID]
	if !ok {
		return nil, nil
	}

	res := make([]Node, len(children))
	copy(res, children)
	return res, nil
}

// TreeGetOpLog implements the pilorama.Forest interface.
func (f *memoryForest) TreeGetOpLog(cid cidSDK.ID, treeID string, height uint64) (Move, error) {
	fullID := cid.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		return Move{}, ErrTreeNotFound
	}

	n := sort.Search(len(s.operations), func(i int) bool {
		return s.operations[i].Time >= height
	})
	if n == len(s.operations) {
		return Move{}, nil
	}
	return s.operations[n].Move, nil
}

// TreeDrop implements the pilorama.Forest interface.
func (f *memoryForest) TreeDrop(cid cidSDK.ID, treeID string) error {
	cidStr := cid.String()
	if treeID == "" {
		for k := range f.treeMap {
			if strings.HasPrefix(k, cidStr) {
				delete(f.treeMap, k)
			}
		}
	} else {
		fullID := cidStr + "/" + treeID
		_, ok := f.treeMap[fullID]
		if !ok {
			return ErrTreeNotFound
		}
		delete(f.treeMap, fullID)
	}
	return nil
}

// TreeList implements the pilorama.Forest interface.
func (f *memoryForest) TreeList(cid cidSDK.ID) ([]string, error) {
	var res []string
	cidStr := cid.EncodeToString()

	for k := range f.treeMap {
		cidAndTree := strings.Split(k, "/")
		if cidAndTree[0] != cidStr {
			continue
		}

		res = append(res, cidAndTree[1])
	}

	return res, nil
}

// TreeExists implements the pilorama.Forest interface.
func (f *memoryForest) TreeExists(cid cidSDK.ID, treeID string) (bool, error) {
	fullID := cid.EncodeToString() + "/" + treeID
	_, ok := f.treeMap[fullID]
	return ok, nil
}
