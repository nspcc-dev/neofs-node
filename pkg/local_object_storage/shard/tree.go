package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

var _ pilorama.Forest = (*Shard)(nil)

// TreeMove implements the pilorama.Forest interface.
func (s *Shard) TreeMove(cid cidSDK.ID, treeID string, m *pilorama.Move) (*pilorama.LogMove, error) {
	return s.pilorama.TreeMove(cid, treeID, m)
}

// TreeAddByPath implements the pilorama.Forest interface.
func (s *Shard) TreeAddByPath(cid cidSDK.ID, treeID string, attr string, path []string, meta []pilorama.KeyValue) ([]pilorama.LogMove, error) {
	return s.pilorama.TreeAddByPath(cid, treeID, attr, path, meta)
}

// TreeApply implements the pilorama.Forest interface.
func (s *Shard) TreeApply(cid cidSDK.ID, treeID string, m *pilorama.Move) error {
	return s.pilorama.TreeApply(cid, treeID, m)
}

// TreeGetByPath implements the pilorama.Forest interface.
func (s *Shard) TreeGetByPath(cid cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]pilorama.Node, error) {
	return s.pilorama.TreeGetByPath(cid, treeID, attr, path, latest)
}

// TreeGetMeta implements the pilorama.Forest interface.
func (s *Shard) TreeGetMeta(cid cidSDK.ID, treeID string, nodeID pilorama.Node) (pilorama.Meta, uint64, error) {
	return s.pilorama.TreeGetMeta(cid, treeID, nodeID)
}

// TreeGetChildren implements the pilorama.Forest interface.
func (s *Shard) TreeGetChildren(cid cidSDK.ID, treeID string, nodeID pilorama.Node) ([]uint64, error) {
	return s.pilorama.TreeGetChildren(cid, treeID, nodeID)
}

// TreeGetOpLog implements the pilorama.Forest interface.
func (s *Shard) TreeGetOpLog(cid cidSDK.ID, treeID string, height uint64) (pilorama.Move, error) {
	return s.pilorama.TreeGetOpLog(cid, treeID, height)
}
