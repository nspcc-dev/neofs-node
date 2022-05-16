package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
)

var _ pilorama.Forest = (*StorageEngine)(nil)

// TreeMove implements the pilorama.Forest interface.
func (e *StorageEngine) TreeMove(d pilorama.CIDDescriptor, treeID string, m *pilorama.Move) (*pilorama.LogMove, error) {
	var err error
	var lm *pilorama.LogMove
	for _, sh := range e.sortShardsByWeight(d.CID) {
		lm, err = sh.TreeMove(d, treeID, m)
		if err != nil {
			e.log.Debug("can't put node in a tree",
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID),
				zap.String("err", err.Error()))
			if errors.Is(err, shard.ErrReadOnlyMode) {
				return nil, err
			}
			continue
		}
		return lm, nil
	}
	return nil, err
}

// TreeAddByPath implements the pilorama.Forest interface.
func (e *StorageEngine) TreeAddByPath(d pilorama.CIDDescriptor, treeID string, attr string, path []string, m []pilorama.KeyValue) ([]pilorama.LogMove, error) {
	var err error
	var lm []pilorama.LogMove
	for _, sh := range e.sortShardsByWeight(d.CID) {
		lm, err = sh.TreeAddByPath(d, treeID, attr, path, m)
		if err != nil {
			e.log.Debug("can't put node in a tree",
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID),
				zap.String("err", err.Error()))
			if errors.Is(err, shard.ErrReadOnlyMode) {
				return nil, err
			}
			continue
		}
		return lm, nil
	}
	return nil, err
}

// TreeApply implements the pilorama.Forest interface.
func (e *StorageEngine) TreeApply(d pilorama.CIDDescriptor, treeID string, m *pilorama.Move) error {
	var err error
	for _, sh := range e.sortShardsByWeight(d.CID) {
		err = sh.TreeApply(d, treeID, m)
		if err != nil {
			e.log.Debug("can't put node in a tree",
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID),
				zap.String("err", err.Error()))
			if errors.Is(err, shard.ErrReadOnlyMode) {
				return err
			}
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
			e.log.Debug("can't put node in a tree",
				zap.Stringer("cid", cid),
				zap.String("tree", treeID),
				zap.String("err", err.Error()))
			continue
		}
		return nodes, nil
	}
	return nil, err
}

// TreeGetMeta implements the pilorama.Forest interface.
func (e *StorageEngine) TreeGetMeta(cid cidSDK.ID, treeID string, nodeID pilorama.Node) (pilorama.Meta, uint64, error) {
	var err error
	var m pilorama.Meta
	var p uint64
	for _, sh := range e.sortShardsByWeight(cid) {
		m, p, err = sh.TreeGetMeta(cid, treeID, nodeID)
		if err != nil {
			e.log.Debug("can't put node in a tree",
				zap.Stringer("cid", cid),
				zap.String("tree", treeID),
				zap.String("err", err.Error()))
			continue
		}
		return m, p, nil
	}
	return pilorama.Meta{}, 0, err
}

// TreeGetChildren implements the pilorama.Forest interface.
func (e *StorageEngine) TreeGetChildren(cid cidSDK.ID, treeID string, nodeID pilorama.Node) ([]uint64, error) {
	var err error
	var nodes []uint64
	for _, sh := range e.sortShardsByWeight(cid) {
		nodes, err = sh.TreeGetChildren(cid, treeID, nodeID)
		if err != nil {
			e.log.Debug("can't put node in a tree",
				zap.Stringer("cid", cid),
				zap.String("tree", treeID),
				zap.String("err", err.Error()))
			continue
		}
		return nodes, nil
	}
	return nil, err
}

// TreeGetOpLog implements the pilorama.Forest interface.
func (e *StorageEngine) TreeGetOpLog(cid cidSDK.ID, treeID string, height uint64) (pilorama.Move, error) {
	var err error
	var lm pilorama.Move
	for _, sh := range e.sortShardsByWeight(cid) {
		lm, err = sh.TreeGetOpLog(cid, treeID, height)
		if err != nil {
			e.log.Debug("can't perform `GetOpLog`",
				zap.Stringer("cid", cid),
				zap.String("tree", treeID),
				zap.Uint64("height", height),
				zap.String("err", err.Error()))
			continue
		}
		return lm, nil
	}
	return lm, err
}
