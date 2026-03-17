package writecache

import (
	"fmt"
	"os"

	coreshard "github.com/nspcc-dev/neofs-node/pkg/core/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/util"
)

func (c *cache) openStore(readOnly bool) error {
	err := util.MkdirAllX(c.path, os.ModePerm)
	if err != nil {
		return err
	}

	c.fsTree = fstree.New(
		fstree.WithPath(c.path),
		fstree.WithPerm(os.ModePerm),
		fstree.WithDepth(1),
		fstree.WithNoSync(c.noSync),
		fstree.WithCombinedCountLimit(1))
	c.fsTree.SetLogger(c.log)
	var id *coreshard.ID
	if c.metrics.id != "" {
		id, err = coreshard.DecodeString(c.metrics.id)
		if err != nil {
			return fmt.Errorf("decode shard ID: %w", err)
		}
	}
	c.fsTree.SetShardID(id)
	if err := c.fsTree.Open(readOnly); err != nil {
		return fmt.Errorf("could not open FSTree: %w", err)
	}

	return nil
}
