package blobstor

import (
	"encoding/hex"
	"io/fs"
	"path/filepath"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// BlobStor represents NeoFS local BLOB storage.
type BlobStor struct {
	cfg

	modeMtx sync.RWMutex
	mode    mode.Mode

	blobovniczas *blobovniczatree.Blobovniczas
}

type Info = fstree.Info

// Option represents BlobStor's constructor option.
type Option func(*cfg)

type cfg struct {
	fsTree fstree.FSTree

	compression.CConfig

	smallSizeLimit uint64

	log *logger.Logger

	blzOpts []blobovniczatree.Option
}

const (
	defaultShallowDepth = 4
	defaultPerm         = 0700

	defaultSmallSizeLimit = 1 << 20 // 1MB
)

const blobovniczaDir = "blobovnicza"

func initConfig(c *cfg) {
	*c = cfg{
		fsTree: fstree.FSTree{
			Depth:      defaultShallowDepth,
			DirNameLen: hex.EncodedLen(fstree.DirNameLen),
			Info: Info{
				Permissions: defaultPerm,
				RootPath:    "./",
			},
		},
		smallSizeLimit: defaultSmallSizeLimit,
		log:            zap.L(),
	}
	c.blzOpts = []blobovniczatree.Option{blobovniczatree.WithCompressionConfig(&c.CConfig)}
}

// New creates, initializes and returns new BlobStor instance.
func New(opts ...Option) *BlobStor {
	bs := new(BlobStor)
	initConfig(&bs.cfg)

	for i := range opts {
		opts[i](&bs.cfg)
	}

	bs.blobovniczas = blobovniczatree.NewBlobovniczaTree(bs.blzOpts...)
	bs.blzOpts = nil

	return bs
}

// SetLogger sets logger. It is used after the shard ID was generated to use it in logs.
func (b *BlobStor) SetLogger(l *zap.Logger) {
	b.log = l
}

// WithShallowDepth returns option to set the
// depth of the object file subdirectory tree.
//
// Depth is reduced to maximum value in case of overflow.
func WithShallowDepth(depth int) Option {
	return func(c *cfg) {
		if depth > fstree.MaxDepth {
			depth = fstree.MaxDepth
		}

		c.fsTree.Depth = depth
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
func WithCompressObjects(comp bool) Option {
	return func(c *cfg) {
		c.Enabled = comp
	}
}

// WithUncompressableContentTypes returns option to disable decompression
// for specific content types as seen by object.AttributeContentType attribute.
func WithUncompressableContentTypes(values []string) Option {
	return func(c *cfg) {
		c.UncompressableContentTypes = values
	}
}

// WithRootPath returns option to set path to root directory
// of the fs tree to write the objects.
func WithRootPath(rootDir string) Option {
	return func(c *cfg) {
		c.fsTree.RootPath = rootDir
		c.blzOpts = append(c.blzOpts, blobovniczatree.WithRootPath(filepath.Join(rootDir, blobovniczaDir)))
	}
}

// WithRootPerm returns option to set permission
// bits of the fs tree.
func WithRootPerm(perm fs.FileMode) Option {
	return func(c *cfg) {
		c.fsTree.Permissions = perm
		c.blzOpts = append(c.blzOpts, blobovniczatree.WithPermissions(perm))
	}
}

// WithSmallSizeLimit returns option to set maximum size of
// "small" object.
func WithSmallSizeLimit(lim uint64) Option {
	return func(c *cfg) {
		c.smallSizeLimit = lim
		c.blzOpts = append(c.blzOpts, blobovniczatree.WithObjectSizeLimit(lim))
	}
}

// WithLogger returns option to specify BlobStor's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l.With(zap.String("component", "BlobStor"))
		c.blzOpts = append(c.blzOpts, blobovniczatree.WithLogger(c.log))
	}
}

// WithBlobovniczaShallowDepth returns option to specify
// depth of blobovnicza directories.
func WithBlobovniczaShallowDepth(d uint64) Option {
	return func(c *cfg) {
		c.blzOpts = append(c.blzOpts, blobovniczatree.WithBlobovniczaShallowDepth(d))
	}
}

// WithBlobovniczaShallowWidth returns option to specify
// width of blobovnicza directories.
func WithBlobovniczaShallowWidth(w uint64) Option {
	return func(c *cfg) {
		c.blzOpts = append(c.blzOpts, blobovniczatree.WithBlobovniczaShallowWidth(w))
	}
}

// WithBlobovniczaOpenedCacheSize return option to specify
// maximum number of opened non-active blobovnicza's.
func WithBlobovniczaOpenedCacheSize(sz int) Option {
	return func(c *cfg) {
		c.blzOpts = append(c.blzOpts, blobovniczatree.WithOpenedCacheSize(sz))
	}
}

// WithBlobovniczaSize returns option to specify maximum volume
// of each blobovnicza.
func WithBlobovniczaSize(sz uint64) Option {
	return func(c *cfg) {
		c.blzOpts = append(c.blzOpts, blobovniczatree.WithBlobovniczaSize(sz))
	}
}
