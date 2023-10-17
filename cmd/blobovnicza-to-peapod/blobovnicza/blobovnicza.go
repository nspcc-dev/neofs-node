package blobovnicza

import (
	"io/fs"
	"os"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// Blobovnicza represents the implementation of NeoFS Blobovnicza.
type Blobovnicza struct {
	cfg

	filled atomic.Uint64

	boltDB *bbolt.DB
}

// Option is an option of Blobovnicza's constructor.
type Option func(*cfg)

type cfg struct {
	boltDBCfg

	fullSizeLimit uint64

	objSizeLimit uint64

	log *zap.Logger
}

type boltDBCfg struct {
	perm fs.FileMode

	path string

	boltOptions *bbolt.Options
}

func defaultCfg(c *cfg) {
	*c = cfg{
		boltDBCfg: boltDBCfg{
			perm: os.ModePerm, // 0777
			boltOptions: &bbolt.Options{
				Timeout: time.Second,
			},
		},
		fullSizeLimit: 1 << 30, // 1GB
		objSizeLimit:  1 << 20, // 1MB
		log:           zap.L(),
	}
}

// New creates and returns a new Blobovnicza instance.
func New(opts ...Option) *Blobovnicza {
	var b Blobovnicza

	defaultCfg(&b.cfg)

	for i := range opts {
		opts[i](&b.cfg)
	}

	return &b
}

// WithPath returns option to set system path to Blobovnicza.
func WithPath(path string) Option {
	return func(c *cfg) {
		c.path = path
	}
}

// WithPermissions returns an option to specify permission bits
// of Blobovnicza's system path.
func WithPermissions(perm fs.FileMode) Option {
	return func(c *cfg) {
		c.perm = perm
	}
}

// WithObjectSizeLimit returns an option to specify the maximum size
// of the objects stored in Blobovnicza.
func WithObjectSizeLimit(lim uint64) Option {
	return func(c *cfg) {
		c.objSizeLimit = lim
	}
}

// WithFullSizeLimit returns an option to set the maximum sum size
// of all stored objects.
func WithFullSizeLimit(lim uint64) Option {
	return func(c *cfg) {
		c.fullSizeLimit = lim
	}
}

// WithLogger returns an option to specify Blobovnicza's logger.
func WithLogger(l *zap.Logger) Option {
	return func(c *cfg) {
		c.log = l.With(zap.String("component", "Blobovnicza"))
	}
}

// WithReadOnly returns an option to open Blobovnicza in read-only mode.
func WithReadOnly(ro bool) Option {
	return func(c *cfg) {
		c.boltOptions.ReadOnly = ro
	}
}
