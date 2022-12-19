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
	index, lst, err := e.getTreeShard(d.CID, treeID)
	if err != nil && !errors.Is(err, pilorama.ErrTreeNotFound) {
		return nil, err
	}

	lm, err := lst[index].TreeMove(d, treeID, m)
	if err != nil {
		if !errors.Is(err, shard.ErrReadOnlyMode) && err != shard.ErrPiloramaDisabled {
			e.reportShardError(lst[index], "can't perform `TreeMove`", err,
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID))
		}

		return nil, err
	}
	return lm, nil
}

// TreeAddByPath implements the pilorama.Forest interface.
func (e *StorageEngine) TreeAddByPath(d pilorama.CIDDescriptor, treeID string, attr string, path []string, m []pilorama.KeyValue) ([]pilorama.LogMove, error) {
	index, lst, err := e.getTreeShard(d.CID, treeID)
	if err != nil && !errors.Is(err, pilorama.ErrTreeNotFound) {
		return nil, err
	}

	lm, err := lst[index].TreeAddByPath(d, treeID, attr, path, m)
	if err != nil {
		if !errors.Is(err, shard.ErrReadOnlyMode) && err != shard.ErrPiloramaDisabled {
			e.reportShardError(lst[index], "can't perform `TreeAddByPath`", err,
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID))
		}
		return nil, err
	}
	return lm, nil
}

// TreeApply implements the pilorama.Forest interface.
func (e *StorageEngine) TreeApply(d pilorama.CIDDescriptor, treeID string, m *pilorama.Move, backgroundSync bool) error {
	index, lst, err := e.getTreeShard(d.CID, treeID)
	if err != nil && !errors.Is(err, pilorama.ErrTreeNotFound) {
		return err
	}

	err = lst[index].TreeApply(d, treeID, m, backgroundSync)
	if err != nil {
		if !errors.Is(err, shard.ErrReadOnlyMode) && err != shard.ErrPiloramaDisabled {
			e.reportShardError(lst[index], "can't perform `TreeApply`", err,
				zap.Stringer("cid", d.CID),
				zap.String("tree", treeID))
		}
		return err
	}
	return nil
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
	var resIDs []string

	for _, sh := range e.unsortedShards() {
		ids, err := sh.TreeList(cid)
		if err != nil {
			if errors.Is(err, shard.ErrPiloramaDisabled) || errors.Is(err, shard.ErrReadOnlyMode) {
				return nil, err
			}

			e.reportShardError(sh, "can't perform `TreeList`", err,
				zap.Stringer("cid", cid))

			// returns as much info about
			// trees as possible
			continue
		}

		resIDs = append(resIDs, ids...)
	}

	return resIDs, nil
}

// TreeExists implements the pilorama.Forest interface.
func (e *StorageEngine) TreeExists(cid cidSDK.ID, treeID string) (bool, error) {
	_, _, err := e.getTreeShard(cid, treeID)
	if errors.Is(err, pilorama.ErrTreeNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (e *StorageEngine) getTreeShard(cid cidSDK.ID, treeID string) (int, []hashedShard, error) {
	lst := e.sortShardsByWeight(cid)
	for i, sh := range lst {
		exists, err := sh.TreeExists(cid, treeID)
		if err != nil {
			return 0, nil, err
		}
		if exists {
			return i, lst, err
		}
	}

	return 0, lst, pilorama.ErrTreeNotFound
}
