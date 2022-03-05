package blobstor

import (
	"encoding/hex"
	"io/fs"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// BlobStor represents NeoFS local BLOB storage.
type BlobStor struct {
	*cfg

	blobovniczas *blobovniczas
}

type Info = fstree.Info

// Option represents BlobStor's constructor option.
type Option func(*cfg)

type cfg struct {
	fsTree fstree.FSTree

	compressionEnabled bool

	uncompressableContentTypes []string

	compressor func([]byte) []byte

	decompressor func([]byte) ([]byte, error)

	smallSizeLimit uint64

	log *logger.Logger

	openedCacheSize int

	blzShallowDepth, blzShallowWidth uint64

	blzRootPath string

	blzOpts []blobovnicza.Option
}

const (
	defaultShallowDepth = 4
	defaultPerm         = 0700

	defaultSmallSizeLimit  = 1 << 20 // 1MB
	defaultOpenedCacheSize = 50
	defaultBlzShallowDepth = 2
	defaultBlzShallowWidth = 16
)

const blobovniczaDir = "blobovnicza"

func defaultCfg() *cfg {
	return &cfg{
		fsTree: fstree.FSTree{
			Depth:      defaultShallowDepth,
			DirNameLen: hex.EncodedLen(fstree.DirNameLen),
			Info: Info{
				Permissions: defaultPerm,
				RootPath:    "./",
			},
		},
		smallSizeLimit:  defaultSmallSizeLimit,
		log:             zap.L(),
		openedCacheSize: defaultOpenedCacheSize,
		blzShallowDepth: defaultBlzShallowDepth,
		blzShallowWidth: defaultBlzShallowWidth,
	}
}

// New creates, initializes and returns new BlobStor instance.
func New(opts ...Option) *BlobStor {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &BlobStor{
		cfg:          c,
		blobovniczas: newBlobovniczaTree(c),
	}
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
		c.compressionEnabled = comp
	}
}

// WithUncompressableContentTypes returns option to disable decompression
// for specific content types as seen by object.AttributeContentType attribute.
func WithUncompressableContentTypes(values []string) Option {
	return func(c *cfg) {
		c.uncompressableContentTypes = values
	}
}

// WithRootPath returns option to set path to root directory
// of the fs tree to write the objects.
func WithRootPath(rootDir string) Option {
	return func(c *cfg) {
		c.fsTree.RootPath = rootDir
		c.blzRootPath = filepath.Join(rootDir, blobovniczaDir)
	}
}

// WithRootPerm returns option to set permission
// bits of the fs tree.
func WithRootPerm(perm fs.FileMode) Option {
	return func(c *cfg) {
		c.fsTree.Permissions = perm
		c.blzOpts = append(c.blzOpts, blobovnicza.WithPermissions(perm))
	}
}

// WithSmallSizeLimit returns option to set maximum size of
// "small" object.
func WithSmallSizeLimit(lim uint64) Option {
	return func(c *cfg) {
		c.smallSizeLimit = lim
		c.blzOpts = append(c.blzOpts, blobovnicza.WithObjectSizeLimit(lim))
	}
}

// WithLogger returns option to specify BlobStor's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l.With(zap.String("component", "BlobStor"))
		c.blzOpts = append(c.blzOpts, blobovnicza.WithLogger(l))
	}
}

// WithBlobovniczaShallowDepth returns option to specify
// depth of blobovnicza directories.
func WithBlobovniczaShallowDepth(d uint64) Option {
	return func(c *cfg) {
		c.blzShallowDepth = d
	}
}

// WithBlobovniczaShallowWidth returns option to specify
// width of blobovnicza directories.
func WithBlobovniczaShallowWidth(w uint64) Option {
	return func(c *cfg) {
		c.blzShallowWidth = w
	}
}

// WithBlobovniczaOpenedCacheSize return option to specify
// maximum number of opened non-active blobovnicza's.
func WithBlobovniczaOpenedCacheSize(sz int) Option {
	return func(c *cfg) {
		c.openedCacheSize = sz
	}
}

// WithBlobovniczaSize returns option to specify maximum volume
// of each blobovnicza.
func WithBlobovniczaSize(sz uint64) Option {
	return func(c *cfg) {
		c.blzOpts = append(c.blzOpts, blobovnicza.WithFullSizeLimit(sz))
	}
}
