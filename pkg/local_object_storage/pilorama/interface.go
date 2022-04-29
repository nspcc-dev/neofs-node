package pilorama

import cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"

// Forest represents CRDT tree.
type Forest interface {
	// TreeMove moves node in the tree.
	// If the parent of the move operation is TrashID, the node is removed.
	// If the child of the move operation is RootID, new ID is generated and added to a tree.
	TreeMove(cid cidSDK.ID, treeID string, m *Move) (*LogMove, error)
	// TreeAddByPath adds new node in the tree using provided path.
	// The path is constructed by descending from the root using the values of the attr in meta.
	TreeAddByPath(cid cidSDK.ID, treeID string, attr string, path []string, meta []KeyValue) ([]LogMove, error)
	// TreeApply applies replicated operation from another node.
	TreeApply(cid cidSDK.ID, treeID string, m *Move) error
	// TreeGetByPath returns all nodes corresponding to the path.
	// The path is constructed by descending from the root using the values of the
	// AttributeFilename in meta.
	// The last argument determines whether only the node with the latest timestamp is returned.
	TreeGetByPath(cid cidSDK.ID, treeID string, attr string, path []string, latest bool) ([]Node, error)
	// TreeGetMeta returns meta information of the node with the specified ID.
	TreeGetMeta(cid cidSDK.ID, treeID string, nodeID Node) (Meta, error)
	// TreeGetChildren returns children of the node with the specified ID. The order is arbitrary.
	TreeGetChildren(cid cidSDK.ID, treeID string, nodeID Node) ([]uint64, error)
}

type ForestStorage interface {
	Init() error
	Open() error
	Close() error
	Forest
}

const (
	AttributeFilename = "FileName"
	AttributeVersion  = "Version"
)
