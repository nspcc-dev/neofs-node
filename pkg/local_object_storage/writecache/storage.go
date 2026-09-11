package writecache

import (
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/util"
)

func (c *cache) newStore(readOnly bool) (*fstree.FSTree, error) {
	if err := util.MkdirAllX(c.path, os.ModePerm); err != nil {
		return nil, err
	}

	fsTree := fstree.New(
		fstree.WithLogger(c.log),
		fstree.WithPath(c.path),
		fstree.WithPerm(os.ModePerm),
		fstree.WithDepth(2),
		fstree.WithAllowDepthChange(true),
		fstree.WithSubtype(wcStorageType),
		fstree.WithNoSync(c.noSync),
		fstree.WithCombinedCountLimit(1))
	if err := fsTree.Open(readOnly); err != nil {
		return nil, fmt.Errorf("could not open FSTree: %w", err)
	}

	return fsTree, nil
}

func (c *cache) openStore(readOnly bool) error {
	fsTree, err := c.newStore(readOnly)
	if err != nil {
		return err
	}
	if c.fsTree != nil {
		if err := c.fsTree.Close(); err != nil {
			return fmt.Errorf("close previous FSTree: %w", err)
		}
	}
	c.fsTree = fsTree

	return nil
}
