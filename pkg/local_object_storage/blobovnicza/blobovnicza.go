package blobovnicza

import (
	"io/fs"
	"os"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.etcd.io/bbolt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Blobovnicza represents the implementation of NeoFS Blobovnicza.
type Blobovnicza struct {
	*cfg

	filled *atomic.Uint64

	boltDB *bbolt.DB
}

// Option is an option of Blobovnicza's constructor.
type Option func(*cfg)

type cfg struct {
	boltDBCfg

	fullSizeLimit uint64

	objSizeLimit uint64

	log *logger.Logger
}

type boltDBCfg struct {
	perm fs.FileMode

	path string

	boltOptions *bbolt.Options
}

func defaultCfg() *cfg {
	return &cfg{
		boltDBCfg: boltDBCfg{
			perm: os.ModePerm, // 0777
			boltOptions: &bbolt.Options{
				Timeout: 100 * time.Millisecond,
			},
		},
		fullSizeLimit: 1 << 30, // 1GB
		objSizeLimit:  1 << 20, // 1MB
		log:           zap.L(),
	}
}

// New creates and returns new Blobovnicza instance.
func New(opts ...Option) *Blobovnicza {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Blobovnicza{
		cfg:    c,
		filled: atomic.NewUint64(0),
	}
}

// WithPath returns option to set system path to Blobovnicza.
func WithPath(path string) Option {
	return func(c *cfg) {
		c.path = path
	}
}

// WithPermissions returns option to specify permission bits
// of Blobovnicza's system path.
func WithPermissions(perm fs.FileMode) Option {
	return func(c *cfg) {
		c.perm = perm
	}
}

// WithObjectSizeLimit returns option to specify maximum size
// of the objects stored in Blobovnicza.
func WithObjectSizeLimit(lim uint64) Option {
	return func(c *cfg) {
		c.objSizeLimit = lim
	}
}

// WithFullSizeLimit returns option to set maximum sum size
// of all stored objects.
func WithFullSizeLimit(lim uint64) Option {
	return func(c *cfg) {
		c.fullSizeLimit = lim
	}
}

// WithLogger returns option to specify Blobovnicza's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l.With(zap.String("component", "Blobovnicza"))
	}
}
