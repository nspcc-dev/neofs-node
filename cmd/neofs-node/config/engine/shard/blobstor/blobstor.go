package blobstorconfig

import (
	"io/fs"
	"math"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
)

const (
	// DepthDefault is the default shallow dir depth.
	DepthDefault = 4
	// CombinedCountLimitDefault is the default for the maximum number of objects to write into a single file.
	CombinedCountLimitDefault = 128
	// CombinedSizeLimitDefault is the default for the maximum size of the combined object file.
	CombinedSizeLimitDefault = 8 * 1024 * 1024
	// CombinedSizeThresholdDefault is the default for the minimal size of the object that won't be combined with others for writes.
	CombinedSizeThresholdDefault = 128 * 1024
	// PermDefault are default permission bits for BlobStor data.
	PermDefault = 0o640
	// DefaultFlushInterval is the default time interval between batch writes to disk.
	DefaultFlushInterval = 10 * time.Millisecond
)

// Blobstor contains configuration for a single BlobStor instance.
type Blobstor struct {
	Type                  string        `mapstructure:"type"`
	Path                  string        `mapstructure:"path"`
	Perm                  fs.FileMode   `mapstructure:"perm"`
	FlushInterval         time.Duration `mapstructure:"flush_interval"`
	Depth                 uint64        `mapstructure:"depth"`
	NoSync                *bool         `mapstructure:"no_sync"`
	CombinedCountLimit    int           `mapstructure:"combined_count_limit"`
	CombinedSizeLimit     internal.Size `mapstructure:"combined_size_limit"`
	CombinedSizeThreshold internal.Size `mapstructure:"combined_size_threshold"`
}

// Normalize fills in default values in Blobstor configuration if they are not set.
//
// It uses the default values from Blobstor itself, and if they are not set too,
// it uses the default values from the package.
func (b *Blobstor) Normalize(def Blobstor) {
	if b.Perm == 0 {
		if def.Perm == 0 {
			b.Perm = PermDefault
		} else {
			b.Perm = def.Perm
		}
	}
	if b.FlushInterval <= 0 {
		if def.FlushInterval <= 0 {
			b.FlushInterval = DefaultFlushInterval
		} else {
			b.FlushInterval = def.FlushInterval
		}
	}
	b.NoSync = internal.CheckPtrBool(b.NoSync, def.NoSync)
	if b.Type == fstree.Type {
		if b.Depth < 1 || b.Depth > fstree.MaxDepth {
			if def.Depth < 1 || def.Depth > fstree.MaxDepth {
				b.Depth = DepthDefault
			} else {
				b.Depth = def.Depth
			}
		}
		if b.CombinedCountLimit <= 0 || b.CombinedCountLimit > math.MaxInt {
			if def.CombinedCountLimit <= 0 || def.CombinedCountLimit > math.MaxInt {
				b.CombinedCountLimit = CombinedCountLimitDefault
			} else {
				b.CombinedCountLimit = def.CombinedCountLimit
			}
		}
		b.CombinedSizeLimit.Check(def.CombinedSizeLimit, CombinedSizeLimitDefault)
		b.CombinedSizeThreshold.Check(def.CombinedSizeThreshold, CombinedSizeThresholdDefault)
	}
}
