package pilorama

import (
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
func (f *memoryForest) TreeMove(cid *cidSDK.ID, treeID string, op *Move) (*LogMove, error) {
	fullID := cid.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		s = newState()
		f.treeMap[fullID] = s
	}

	op.Time = s.timestamp()
	if op.Child == RootID {
		op.Child = s.findSpareID()
	}

	lm := s.do(op)
	s.operations = append(s.operations, lm)
	return &lm, nil
}

// TreeAddByPath implements the Forest interface.
func (f *memoryForest) TreeAddByPath(cid *cidSDK.ID, treeID string, attr string, path []string, m []KeyValue) ([]LogMove, error) {
	fullID := cid.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		s = newState()
		f.treeMap[fullID] = s
	}

	i, node := s.getPathPrefix(attr, path)
	lm := make([]LogMove, len(path)-i+1)
	for j := i; j < len(path); j++ {
		lm[j-i] = s.do(&Move{
			Parent: node,
			Meta:   Meta{Time: s.timestamp(), Items: []KeyValue{{Key: attr, Value: []byte(path[j])}}},
			Child:  s.findSpareID(),
		})
		node = lm[j-i].Child
		s.operations = append(s.operations, lm[j-i])
	}
	lm[len(lm)-1] = s.do(&Move{
		Parent: node,
		Meta:   Meta{Time: s.timestamp(), Items: m},
		Child:  s.findSpareID(),
	})
	return lm, nil
}

// TreeApply implements the Forest interface.
func (f *memoryForest) TreeApply(cid *cidSDK.ID, treeID string, op *Move) error {
	fullID := cid.String() + "/" + treeID
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

func (f *memoryForest) Open() error {
	return nil
}

func (f *memoryForest) Close() error {
	return nil
}

// TreeGetByPath implements the Forest interface.
func (f *memoryForest) TreeGetByPath(cid *cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]Node, error) {
	fullID := cid.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		return nil, ErrTreeNotFound
	}

	return s.get(attr, path, latest), nil
}

// TreeGetMeta implements the Forest interface.
func (f *memoryForest) TreeGetMeta(cid *cidSDK.ID, treeID string, nodeID Node) (Meta, error) {
	fullID := cid.String() + "/" + treeID
	s, ok := f.treeMap[fullID]
	if !ok {
		return Meta{}, ErrTreeNotFound
	}

	return s.getMeta(nodeID), nil
}
