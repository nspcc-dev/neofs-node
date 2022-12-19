package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

var _ pilorama.Forest = (*Shard)(nil)

// ErrPiloramaDisabled is returned when pilorama was disabled in the configuration.
var ErrPiloramaDisabled = logicerr.New("pilorama is disabled")

// TreeMove implements the pilorama.Forest interface.
func (s *Shard) TreeMove(d pilorama.CIDDescriptor, treeID string, m *pilorama.Move) (*pilorama.LogMove, error) {
	if s.pilorama == nil {
		return nil, ErrPiloramaDisabled
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.ReadOnly() {
		return nil, ErrReadOnlyMode
	}
	return s.pilorama.TreeMove(d, treeID, m)
}

// TreeAddByPath implements the pilorama.Forest interface.
func (s *Shard) TreeAddByPath(d pilorama.CIDDescriptor, treeID string, attr string, path []string, meta []pilorama.KeyValue) ([]pilorama.LogMove, error) {
	if s.pilorama == nil {
		return nil, ErrPiloramaDisabled
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.ReadOnly() {
		return nil, ErrReadOnlyMode
	}
	return s.pilorama.TreeAddByPath(d, treeID, attr, path, meta)
}

// TreeApply implements the pilorama.Forest interface.
func (s *Shard) TreeApply(d pilorama.CIDDescriptor, treeID string, m *pilorama.Move, backgroundSync bool) error {
	if s.pilorama == nil {
		return ErrPiloramaDisabled
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.ReadOnly() {
		return ErrReadOnlyMode
	}
	return s.pilorama.TreeApply(d, treeID, m, backgroundSync)
}

// TreeGetByPath implements the pilorama.Forest interface.
func (s *Shard) TreeGetByPath(cid cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]pilorama.Node, error) {
	if s.pilorama == nil {
		return nil, ErrPiloramaDisabled
	}
	return s.pilorama.TreeGetByPath(cid, treeID, attr, path, latest)
}

// TreeGetMeta implements the pilorama.Forest interface.
func (s *Shard) TreeGetMeta(cid cidSDK.ID, treeID string, nodeID pilorama.Node) (pilorama.Meta, uint64, error) {
	if s.pilorama == nil {
		return pilorama.Meta{}, 0, ErrPiloramaDisabled
	}
	return s.pilorama.TreeGetMeta(cid, treeID, nodeID)
}

// TreeGetChildren implements the pilorama.Forest interface.
func (s *Shard) TreeGetChildren(cid cidSDK.ID, treeID string, nodeID pilorama.Node) ([]uint64, error) {
	if s.pilorama == nil {
		return nil, ErrPiloramaDisabled
	}
	return s.pilorama.TreeGetChildren(cid, treeID, nodeID)
}

// TreeGetOpLog implements the pilorama.Forest interface.
func (s *Shard) TreeGetOpLog(cid cidSDK.ID, treeID string, height uint64) (pilorama.Move, error) {
	if s.pilorama == nil {
		return pilorama.Move{}, ErrPiloramaDisabled
	}
	return s.pilorama.TreeGetOpLog(cid, treeID, height)
}

// TreeDrop implements the pilorama.Forest interface.
func (s *Shard) TreeDrop(cid cidSDK.ID, treeID string) error {
	if s.pilorama == nil {
		return ErrPiloramaDisabled
	}
	return s.pilorama.TreeDrop(cid, treeID)
}

// TreeList implements the pilorama.Forest interface.
func (s *Shard) TreeList(cid cidSDK.ID) ([]string, error) {
	if s.pilorama == nil {
		return nil, ErrPiloramaDisabled
	}
	return s.pilorama.TreeList(cid)
}

// TreeExists implements the pilorama.Forest interface.
func (s *Shard) TreeExists(cid cidSDK.ID, treeID string) (bool, error) {
	if s.pilorama == nil {
		return false, ErrPiloramaDisabled
	}
	return s.pilorama.TreeExists(cid, treeID)
}
