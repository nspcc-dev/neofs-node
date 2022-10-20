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
			if errors.Is(err, shard.ErrReadOnlyMode) || err == shard.ErrPiloramaDisabled {
				return nil, err
			}
			e.reportShardError(sh, "can't perform `TreeMove`", err,
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID))
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
			if errors.Is(err, shard.ErrReadOnlyMode) || err == shard.ErrPiloramaDisabled {
				return nil, err
			}
			e.reportShardError(sh, "can't perform `TreeAddByPath`", err,
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID))
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
			if errors.Is(err, shard.ErrReadOnlyMode) || err == shard.ErrPiloramaDisabled {
				return err
			}
			e.reportShardError(sh, "can't perform `TreeApply`", err,
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID))
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
			if err == shard.ErrPiloramaDisabled {
				break
			}
			if !errors.Is(err, pilorama.ErrTreeNotFound) {
				e.reportShardError(sh, "can't perform `TreeGetByPath`", err,
					zap.Stringer("cid", cid),
					zap.String("tree", treeID))
			}
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
			if err == shard.ErrPiloramaDisabled {
				break
			}
			if !errors.Is(err, pilorama.ErrTreeNotFound) {
				e.reportShardError(sh, "can't perform `TreeGetMeta`", err,
					zap.Stringer("cid", cid),
					zap.String("tree", treeID))
			}
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
			if err == shard.ErrPiloramaDisabled {
				break
			}
			if !errors.Is(err, pilorama.ErrTreeNotFound) {
				e.reportShardError(sh, "can't perform `TreeGetChildren`", err,
					zap.Stringer("cid", cid),
					zap.String("tree", treeID))
			}
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
			if err == shard.ErrPiloramaDisabled {
				break
			}
			if !errors.Is(err, pilorama.ErrTreeNotFound) {
				e.reportShardError(sh, "can't perform `TreeGetOpLog`", err,
					zap.Stringer("cid", cid),
					zap.String("tree", treeID))
			}
			continue
		}
		return lm, nil
	}
	return lm, err
}

// TreeDrop implements the pilorama.Forest interface.
func (e *StorageEngine) TreeDrop(cid cidSDK.ID, treeID string) error {
	var err error
	for _, sh := range e.sortShardsByWeight(cid) {
		err = sh.TreeDrop(cid, treeID)
		if err != nil {
			if err == shard.ErrPiloramaDisabled {
				break
			}
			if !errors.Is(err, pilorama.ErrTreeNotFound) && !errors.Is(err, shard.ErrReadOnlyMode) {
				e.reportShardError(sh, "can't perform `TreeDrop`", err,
					zap.Stringer("cid", cid),
					zap.String("tree", treeID))
			}
			continue
		}
		return nil
	}
	return err
}

// TreeList implements the pilorama.Forest interface.
func (e *StorageEngine) TreeList(cid cidSDK.ID) ([]string, error) {
	//TODO implement me
	panic("implement me")
}
