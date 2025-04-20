package meta

import (
	"context"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// EpochState is an interface that provides access to the
// current epoch number.
type EpochState interface {
	// CurrentEpoch must return current epoch height.
	CurrentEpoch() uint64
}

// Containers provides access to information about NeoFS containers required for
// DB to process.
type Containers interface {
	// Exists checks presence of the referenced container.
	Exists(cid.ID) (bool, error)
}

// DB represents local metabase of storage node.
type DB struct {
	*cfg

	modeMtx sync.RWMutex
	mode    mode.Mode

	boltDB *bbolt.DB
}

// Option is an option of DB constructor.
type Option func(*cfg)

type cfg struct {
	boltOptions *bbolt.Options // optional

	boltBatchSize  int
	boltBatchDelay time.Duration

	info Info

	log *zap.Logger

	epochState EpochState
	containers Containers

	initCtx context.Context
}

func defaultCfg() *cfg {
	return &cfg{
		info: Info{
			Permission: os.ModePerm, // 0777
		},
		boltBatchDelay: 10 * time.Millisecond,
		boltBatchSize:  1000,
		log:            zap.L(),
		initCtx:        context.Background(),
	}
}

// New creates and returns new Metabase instance.
func New(opts ...Option) *DB {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	if c.epochState == nil {
		panic("metabase: epoch state is not specified")
	}

	return &DB{
		cfg: c,
	}
}

// SetLogger sets logger. It is used after the shard ID was generated to use it in logs.
func (db *DB) SetLogger(l *zap.Logger) {
	db.log = l
}

// WithLogger returns option to set logger of DB.
func WithLogger(l *zap.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}

// WithBoltDBOptions returns option to specify BoltDB options.
func WithBoltDBOptions(opts *bbolt.Options) Option {
	return func(c *cfg) {
		c.boltOptions = opts
	}
}

// WithPath returns option to set system path to Metabase.
func WithPath(path string) Option {
	return func(c *cfg) {
		c.info.Path = path
	}
}

// WithPermissions returns option to specify permission bits
// of Metabase system path.
func WithPermissions(perm fs.FileMode) Option {
	return func(c *cfg) {
		c.info.Permission = perm
	}
}

// WithMaxBatchSize returns option to specify maximum concurrent operations
// to be processed in a single transactions.
// This option is missing from `bbolt.Options` but is set right after DB is open.
func WithMaxBatchSize(s int) Option {
	return func(c *cfg) {
		if s != 0 {
			c.boltBatchSize = s
		}
	}
}

// WithMaxBatchDelay returns option to specify maximum time to wait before
// the batch of concurrent transactions is processed.
// This option is missing from `bbolt.Options` but is set right after DB is open.
func WithMaxBatchDelay(d time.Duration) Option {
	return func(c *cfg) {
		if d != 0 {
			c.boltBatchDelay = d
		}
	}
}

// WithEpochState return option to specify a source of current epoch height.
func WithEpochState(s EpochState) Option {
	return func(c *cfg) {
		c.epochState = s
	}
}

// WithContainers return option to specify container source.
func WithContainers(cs Containers) Option { return func(c *cfg) { c.containers = cs } }

// WithInitContext allows to specify context for [DB.Init] operation.
func WithInitContext(ctx context.Context) Option {
	// TODO: accept context parameter instead. Current approach allowed to reduce
	//  code changes.
	return func(c *cfg) { c.initCtx = ctx }
}
