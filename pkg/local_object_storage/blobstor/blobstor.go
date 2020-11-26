package blobstor

import (
	"encoding/hex"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// BlobStor represents NeoFS local BLOB storage.
type BlobStor struct {
	*cfg
}

// Option represents BlobStor's constructor option.
type Option func(*cfg)

type cfg struct {
	fsTree fsTree

	compressor func([]byte) []byte

	decompressor func([]byte) ([]byte, error)

	smallSizeLimit uint64
}

const (
	defaultShallowDepth = 4
	defaultPerm         = 0700
)

func defaultCfg() *cfg {
	return &cfg{
		fsTree: fsTree{
			depth:      defaultShallowDepth,
			dirNameLen: hex.EncodedLen(dirNameLen),
			Info: Info{
				Permissions: defaultPerm,
				RootPath:    "./",
			},
		},
		compressor:     noOpCompressor,
		decompressor:   noOpDecompressor,
		smallSizeLimit: 1 << 20,
	}
}

// New creates, initializes and returns new BlobStor instance.
func New(opts ...Option) *BlobStor {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &BlobStor{
		cfg: c,
	}
}

// WithShallowDepth returns option to set the
// depth of the object file subdirectory tree.
//
// Depth is reduced to maximum value in case of overflow.
func WithShallowDepth(depth int) Option {
	return func(c *cfg) {
		if depth > maxDepth {
			depth = maxDepth
		}

		c.fsTree.depth = depth
	}
}

// WithCompressObjects returns option to toggle
// compression of the stored objects.
//
// If true, Zstandard algorithm is used for data compression.
//
// If compressor (decompressor) creation failed,
// the uncompressed option will be used, and the error
// is recorded in the provided log.
func WithCompressObjects(comp bool, log *logger.Logger) Option {
	return func(c *cfg) {
		if comp {
			var err error

			if c.compressor, err = zstdCompressor(); err != nil {
				log.Error("could not create zstd compressor",
					zap.String("error", err.Error()),
				)
			} else if c.decompressor, err = zstdDecompressor(); err != nil {
				log.Error("could not create zstd decompressor",
					zap.String("error", err.Error()),
				)
			} else {
				return
			}
		}

		c.compressor = noOpCompressor
		c.decompressor = noOpDecompressor
	}
}

// WithTreeRootPath returns option to set path to root directory
// of the fs tree to write the objects.
func WithTreeRootPath(rootDir string) Option {
	return func(c *cfg) {
		c.fsTree.RootPath = rootDir
	}
}

// WithTreeRootPerm returns option to set permission
// bits of the fs tree.
func WithTreeRootPerm(perm os.FileMode) Option {
	return func(c *cfg) {
		c.fsTree.Permissions = perm
	}
}

// WithSmallSizeLimit returns option to set maximum size of
// "small" object.
func WithSmallSizeLimit(lim uint64) Option {
	return func(c *cfg) {
		c.smallSizeLimit = lim
	}
}
