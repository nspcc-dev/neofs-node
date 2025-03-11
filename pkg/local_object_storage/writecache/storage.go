package writecache

import (
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/util"
)

const dbName = "small.bolt"

func (c *cache) openStore(readOnly bool) error {
	err := util.MkdirAllX(c.path, os.ModePerm)
	if err != nil {
		return err
	}

	c.fsTree = fstree.New(
		fstree.WithPath(c.path),
		fstree.WithPerm(os.ModePerm),
		fstree.WithDepth(1),
		fstree.WithDirNameLen(1),
		fstree.WithNoSync(c.noSync),
		fstree.WithCombinedCountLimit(1))
	if err := c.fsTree.Open(readOnly); err != nil {
		return fmt.Errorf("could not open FSTree: %w", err)
	}

	return nil
}
