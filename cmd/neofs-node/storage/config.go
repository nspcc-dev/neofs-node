package storage

import (
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	shardmode "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

type ShardCfg struct {
	Compress                  bool
	SmallSizeObjectLimit      uint64
	UncompressableContentType []string
	RefillMetabase            bool
	Mode                      shardmode.Mode

	MetaCfg struct {
		Path          string
		Perm          fs.FileMode
		MaxBatchSize  int
		MaxBatchDelay time.Duration
	}

	SubStorages []SubStorageCfg

	GcCfg struct {
		RemoverBatchSize     int
		RemoverSleepInterval time.Duration
	}

	WritecacheCfg struct {
		Enabled          bool
		Path             string
		MaxBatchSize     int
		MaxBatchDelay    time.Duration
		SmallObjectSize  uint64
		MaxObjSize       uint64
		FlushWorkerCount int
		SizeLimit        uint64
		NoSync           bool
	}

	PiloramaCfg struct {
		Enabled       bool
		Path          string
		Perm          fs.FileMode
		NoSync        bool
		MaxBatchSize  int
		MaxBatchDelay time.Duration
	}
}
type SubStorageCfg struct {
	// common for all storages
	Typ  string
	Path string
	Perm fs.FileMode

	// tree-specific (FS)
	Depth  uint64
	NoSync bool

	// Peapod-specific
	FlushInterval time.Duration
}

// ID returns persistent id of a shard. It is different from the ID used in runtime
// and is primarily used to identify shards in the configuration.
func (c *ShardCfg) ID() string {
	// This calculation should be kept in sync with
	// pkg/local_object_storage/engine/control.go file.
	var sb strings.Builder
	for i := range c.SubStorages {
		sb.WriteString(filepath.Clean(c.SubStorages[i].Path))
	}
	return sb.String()
}
