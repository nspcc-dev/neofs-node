package blobovniczatree

import (
	"io/fs"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

type cfg struct {
	log             *logger.Logger
	perm            fs.FileMode
	readOnly        bool
	rootPath        string
	openedCacheSize int
	blzShallowDepth uint64
	blzShallowWidth uint64
	compression     *compression.Config
	blzOpts         []blobovnicza.Option
	// reportError is the function called when encountering disk errors.
	reportError func(string, error)
}

type Option func(*cfg)

const (
	defaultPerm            = 0700
	defaultOpenedCacheSize = 50
	defaultBlzShallowDepth = 2
	defaultBlzShallowWidth = 16
)

func initConfig(c *cfg) {
	*c = cfg{
		log:             &logger.Logger{Logger: zap.L()},
		perm:            defaultPerm,
		openedCacheSize: defaultOpenedCacheSize,
		blzShallowDepth: defaultBlzShallowDepth,
		blzShallowWidth: defaultBlzShallowWidth,
		reportError:     func(string, error) {},
	}
}

func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
		c.blzOpts = append(c.blzOpts, blobovnicza.WithLogger(l))
	}
}

func WithPermissions(perm fs.FileMode) Option {
	return func(c *cfg) {
		c.perm = perm
	}
}

func WithBlobovniczaShallowWidth(width uint64) Option {
	return func(c *cfg) {
		c.blzShallowWidth = width
	}
}

func WithBlobovniczaShallowDepth(depth uint64) Option {
	return func(c *cfg) {
		c.blzShallowDepth = depth
	}
}

func WithRootPath(p string) Option {
	return func(c *cfg) {
		c.rootPath = p
	}
}

func WithBlobovniczaSize(sz uint64) Option {
	return func(c *cfg) {
		c.blzOpts = append(c.blzOpts, blobovnicza.WithFullSizeLimit(sz))
	}
}

func WithOpenedCacheSize(sz int) Option {
	return func(c *cfg) {
		c.openedCacheSize = sz
	}
}

func WithObjectSizeLimit(sz uint64) Option {
	return func(c *cfg) {
		c.blzOpts = append(c.blzOpts, blobovnicza.WithObjectSizeLimit(sz))
	}
}
