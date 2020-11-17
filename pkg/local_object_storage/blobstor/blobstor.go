package blobstor

import (
	"encoding/hex"
	"os"
	"sync"
)

// BlobStor represents NeoFS local BLOB storage.
type BlobStor struct {
	*cfg

	mtx *sync.RWMutex
}

// Option represents BlobStor's constructor option.
type Option func(*cfg)

type cfg struct {
	fsTree fsTree

	compressor func([]byte) []byte

	decompressor func([]byte) ([]byte, error)
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
			perm:       defaultPerm,
			rootDir:    "./",
		},
		compressor:   noOpCompressor,
		decompressor: noOpDecompressor,
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
		if depth <= maxDepth {
			depth = maxDepth
		}

		c.fsTree.depth = depth
	}
}

// WithCompressObjects returns option to toggle
// compression of the stored objects.
func WithCompressObjects(comp bool) Option {
	return func(c *cfg) {
		if comp {
			c.compressor = zstdCompressor()
			c.decompressor = zstdDecompressor()
		} else {
			c.compressor = noOpCompressor
			c.decompressor = noOpDecompressor
		}
	}
}

// WithTreeRootPath returns option to set path to root directory
// of the fs tree to write the objects.
func WithTreeRootPath(rootDir string) Option {
	return func(c *cfg) {
		c.fsTree.rootDir = rootDir
	}
}

// WithTreeRootPerm returns option to set permission
// bits of the fs tree.
func WithTreeRootPerm(perm os.FileMode) Option {
	return func(c *cfg) {
		c.fsTree.perm = perm
	}
}
