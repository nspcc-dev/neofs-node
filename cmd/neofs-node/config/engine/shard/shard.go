package shardconfig

import (
	"path/filepath"
	"strings"

	blobstorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor"
	gcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/gc"
	metabaseconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/metabase"
	writecacheconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/writecache"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// SmallSizeLimitDefault is the default limit of small objects payload in bytes.
const SmallSizeLimitDefault = 1 << 20

// ShardDetails contains configuration for a single shard of a storage node.
type ShardDetails struct {
	Mode                           mode.Mode     `mapstructure:"mode"`
	ResyncMetabase                 *bool         `mapstructure:"resync_metabase"`
	Compress                       *bool         `mapstructure:"compress"`
	CompressionExcludeContentTypes []string      `mapstructure:"compression_exclude_content_types"`
	SmallObjectSize                internal.Size `mapstructure:"small_object_size"`

	WriteCache writecacheconfig.WriteCache `mapstructure:"writecache"`
	Metabase   metabaseconfig.Metabase     `mapstructure:"metabase"`
	Blobstor   blobstorconfig.Blobstor     `mapstructure:"blobstor"`
	GC         gcconfig.GC                 `mapstructure:"gc"`
}

// Normalize ensures that all fields of ShardDetails have valid values.
// If some of fields are not set or have invalid values, they will be
// set to default values.
func (s *ShardDetails) Normalize(def ShardDetails) {
	s.ResyncMetabase = internal.CheckPtrBool(s.ResyncMetabase, def.ResyncMetabase)
	s.Compress = internal.CheckPtrBool(s.Compress, def.Compress)
	s.SmallObjectSize.Check(def.SmallObjectSize, SmallSizeLimitDefault)
	s.Blobstor.Normalize(def.Blobstor)
	s.WriteCache.Normalize(def.WriteCache)
	s.Metabase.Normalize(def.Metabase)
	s.GC.Normalize(def.GC)
}

// ID returns persistent id of a shard. It is different from the ID used in runtime
// and is primarily used to identify shards in the configuration.
func (c *ShardDetails) ID() string {
	// This calculation should be kept in sync with
	// pkg/local_object_storage/engine/control.go file.
	var sb strings.Builder
	sb.WriteString(filepath.Clean(c.Blobstor.Path))
	return sb.String()
}
