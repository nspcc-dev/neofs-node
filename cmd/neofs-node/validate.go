package main

import (
	"fmt"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	loggerconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/logger"
	treeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/tree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// validateConfig validates storage node configuration.
func validateConfig(c *config.Config) error {
	// logger configuration validation

	var loggerPrm logger.Prm

	err := loggerPrm.SetLevelString(loggerconfig.Level(c))
	if err != nil {
		return fmt.Errorf("invalid logger level: %w", err)
	}

	// shard configuration validation

	shardNum := 0
	paths := make(map[string]pathDescription)
	return engineconfig.IterateShards(c, false, func(sc *shardconfig.Config) error {
		if sc.WriteCache().Enabled() {
			err := addPath(paths, "writecache", shardNum, sc.WriteCache().Path())
			if err != nil {
				return err
			}
		}

		if err := addPath(paths, "metabase", shardNum, sc.Metabase().Path()); err != nil {
			return err
		}

		treeConfig := treeconfig.Tree(c)
		if treeConfig.Enabled() {
			err := addPath(paths, "pilorama", shardNum, sc.Pilorama().Path())
			if err != nil {
				return err
			}
		}

		blobstor := sc.BlobStor().Storages()
		if len(blobstor) != 2 {
			// TODO (@fyrcik): remove after #1522
			return fmt.Errorf("blobstor section must have 2 components, got: %d", len(blobstor))
		}
		for i := range blobstor {
			switch blobstor[i].Type() {
			case fstree.Type, blobovniczatree.Type:
			default:
				// FIXME #1764 (@fyrchik): this line is currently unreachable,
				//   because we panic in `sc.BlobStor().Storages()`.
				return fmt.Errorf("unexpected storage type: %s (shard %d)",
					blobstor[i].Type(), shardNum)
			}
			if blobstor[i].Perm()&0600 != 0600 {
				return fmt.Errorf("invalid permissions for blobstor component: %s, "+
					"expected at least rw- for the owner (shard %d)",
					blobstor[i].Perm(), shardNum)
			}
			if blobstor[i].Path() == "" {
				return fmt.Errorf("blobstor component path is empty (shard %d)", shardNum)
			}
			err := addPath(paths, fmt.Sprintf("blobstor[%d]", i), shardNum, blobstor[i].Path())
			if err != nil {
				return err
			}
		}

		shardNum++
		return nil
	})
}

type pathDescription struct {
	shard     int
	component string
}

func addPath(paths map[string]pathDescription, component string, shard int, path string) error {
	if path == "" {
		return fmt.Errorf("%s at shard %d has empty path", component, shard)
	}

	path = filepath.Clean(path)
	c, ok := paths[path]
	if ok {
		return fmt.Errorf("%s at shard %d and %s at shard %d have the same paths: %s",
			c.component, c.shard, component, shard, path)
	}

	paths[path] = pathDescription{shard: shard, component: component}
	return nil
}
