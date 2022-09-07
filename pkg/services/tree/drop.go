package tree

import (
	"context"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// DropTree drops a tree from the database. If treeID is empty, all the trees are dropped.
func (s *Service) DropTree(_ context.Context, cid cid.ID, treeID string) error {
	// The only current use-case is a container removal, where all trees should be removed.
	// Thus there is no need to replicate the operation on other node.
	return s.forest.TreeDrop(cid, treeID)
}
