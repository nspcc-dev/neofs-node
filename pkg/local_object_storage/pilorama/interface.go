package pilorama

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// Forest represents CRDT tree.
type Forest interface {
	// TreeMove moves node in the tree.
	// If the parent of the move operation is TrashID, the node is removed.
	// If the child of the move operation is RootID, new ID is generated and added to a tree.
	TreeMove(d CIDDescriptor, treeID string, m *Move) (*LogMove, error)
	// TreeAddByPath adds new node in the tree using provided path.
	// The path is constructed by descending from the root using the values of the attr in meta.
	// Internal nodes in path should have exactly one attribute, otherwise a new node is created.
	TreeAddByPath(d CIDDescriptor, treeID string, attr string, path []string, meta []KeyValue) ([]LogMove, error)
	// TreeApply applies replicated operation from another node.
	// If background is true, TreeApply will first check whether an operation exists.
	TreeApply(d CIDDescriptor, treeID string, m *Move, backgroundSync bool) error
	// TreeGetByPath returns all nodes corresponding to the path.
	// The path is constructed by descending from the root using the values of the
	// AttributeFilename in meta.
	// The last argument determines whether only the node with the latest timestamp is returned.
	// Should return ErrTreeNotFound if the tree is not found, and empty result if the path is not in the tree.
	TreeGetByPath(cid cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]Node, error)
	// TreeGetMeta returns meta information of the node with the specified ID.
	// Should return ErrTreeNotFound if the tree is not found, and empty result if the node is not in the tree.
	TreeGetMeta(cid cidSDK.ID, treeID string, nodeID Node) (Meta, Node, error)
	// TreeGetChildren returns children of the node with the specified ID. The order is arbitrary.
	// Should return ErrTreeNotFound if the tree is not found, and empty result if the node is not in the tree.
	TreeGetChildren(cid cidSDK.ID, treeID string, nodeID Node) ([]uint64, error)
	// TreeGetOpLog returns first log operation stored at or above the height.
	// In case no such operation is found, empty Move and nil error should be returned.
	TreeGetOpLog(cid cidSDK.ID, treeID string, height uint64) (Move, error)
	// TreeDrop drops a tree from the database.
	// If the tree is not found, ErrTreeNotFound should be returned.
	// In case of empty treeID drops all trees related to container.
	TreeDrop(cid cidSDK.ID, treeID string) error
	// TreeList returns all the tree IDs that have been added to the
	// passed container ID. Nil slice should be returned if no tree found.
	TreeList(cid cidSDK.ID) ([]string, error)
	// TreeExists checks if a tree exists locally.
	// If the tree is not found, false and a nil error should be returned.
	TreeExists(cid cidSDK.ID, treeID string) (bool, error)
}

type ForestStorage interface {
	// DumpInfo returns information about the pilorama.
	DumpInfo() Info
	Init() error
	Open(bool) error
	Close() error
	SetMode(m mode.Mode) error
	Forest
}

const (
	AttributeFilename = "FileName"
	AttributeVersion  = "Version"
)

// CIDDescriptor contains container ID and information about the node position
// in the list of container nodes.
type CIDDescriptor struct {
	CID      cidSDK.ID
	Position int
	Size     int
}

// ErrInvalidCIDDescriptor is returned when info about tne node position
// in the container is invalid.
var ErrInvalidCIDDescriptor = logicerr.New("cid descriptor is invalid")

func (d CIDDescriptor) checkValid() bool {
	return 0 <= d.Position && d.Position < d.Size
}
