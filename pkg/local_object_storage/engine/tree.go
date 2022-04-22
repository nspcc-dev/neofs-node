package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

var _ pilorama.Forest = (*StorageEngine)(nil)

// TreeMove implements the pilorama.Forest interface.
func (e *StorageEngine) TreeMove(cid cidSDK.ID, treeID string, m *pilorama.Move) (*pilorama.LogMove, error) {
	var err error
	var lm *pilorama.LogMove
	for _, sh := range e.sortShardsByWeight(cid) {
		lm, err = sh.TreeMove(cid, treeID, m)
		if err != nil {
			continue
		}
		return lm, nil
	}
	return nil, err
}

// TreeAddByPath implements the pilorama.Forest interface.
func (e *StorageEngine) TreeAddByPath(cid cidSDK.ID, treeID string, attr string, path []string, m []pilorama.KeyValue) ([]pilorama.LogMove, error) {
	var err error
	var lm []pilorama.LogMove
	for _, sh := range e.sortShardsByWeight(cid) {
		lm, err = sh.TreeAddByPath(cid, treeID, attr, path, m)
		if err != nil {
			continue
		}
		return lm, nil
	}
	return nil, err
}

// TreeApply implements the pilorama.Forest interface.
func (e *StorageEngine) TreeApply(cid cidSDK.ID, treeID string, m *pilorama.Move) error {
	var err error
	for _, sh := range e.sortShardsByWeight(cid) {
		err = sh.TreeApply(cid, treeID, m)
		if err != nil {
			continue
		}
		return nil
	}

	return err
}

// TreeGetByPath implements the pilorama.Forest interface.
func (e *StorageEngine) TreeGetByPath(cid cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]pilorama.Node, error) {
	var err error
	var nodes []pilorama.Node
	for _, sh := range e.sortShardsByWeight(cid) {
		nodes, err = sh.TreeGetByPath(cid, treeID, attr, path, latest)
		if err != nil {
			continue
		}
		return nodes, nil
	}
	return nil, err
}

// TreeGetMeta implements the pilorama.Forest interface.
func (e *StorageEngine) TreeGetMeta(cid cidSDK.ID, treeID string, nodeID pilorama.Node) (pilorama.Meta, error) {
	var err error
	var m pilorama.Meta
	for _, sh := range e.sortShardsByWeight(cid) {
		m, err = sh.TreeGetMeta(cid, treeID, nodeID)
		if err != nil {
			continue
		}
		return m, nil
	}
	return pilorama.Meta{}, err
}
