package putsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

type MaxSizeSource interface {
	// MaxObjectSize returns maximum payload size
	// of physically stored object in system.
	//
	// Must return 0 if value can not be obtained.
	MaxObjectSize() uint64
}

type Service struct {
	*cfg
}

type Option func(*cfg)

type ClientConstructor interface {
	Get(network.AddressGroup) (client.Client, error)
}

type cfg struct {
	keyStorage *objutil.KeyStorage

	maxSizeSrc MaxSizeSource

	localStore *engine.StorageEngine

	cnrSrc container.Source

	netMapSrc netmap.Source

	remotePool, localPool util.WorkerPool

	netmapKeys netmap.AnnouncedKeys

	fmtValidator *object.FormatValidator

	fmtValidatorOpts []object.FormatValidatorOption

	networkState netmap.State

	clientConstructor ClientConstructor

	log *logger.Logger
}

func defaultCfg() *cfg {
	return &cfg{
		remotePool: new(util.SyncWorkerPool),
		localPool:  new(util.SyncWorkerPool),
		log:        zap.L(),
	}
}

func NewService(opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	c.fmtValidator = object.NewFormatValidator(c.fmtValidatorOpts...)

	return &Service{
		cfg: c,
	}
}

func (p *Service) Put(ctx context.Context) (*Streamer, error) {
	return &Streamer{
		cfg: p.cfg,
		ctx: ctx,
	}, nil
}

func WithKeyStorage(v *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = v
	}
}

func WithMaxSizeSource(v MaxSizeSource) Option {
	return func(c *cfg) {
		c.maxSizeSrc = v
	}
}

func WithLocalStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localStore = v
	}
}

func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.cnrSrc = v
	}
}

func WithNetworkMapSource(v netmap.Source) Option {
	return func(c *cfg) {
		c.netMapSrc = v
	}
}

func WithWorkerPools(remote, local util.WorkerPool) Option {
	return func(c *cfg) {
		c.remotePool, c.localPool = remote, local
	}
}

func WithNetmapKeys(v netmap.AnnouncedKeys) Option {
	return func(c *cfg) {
		c.netmapKeys = v
	}
}

func WithFormatValidatorOpts(v ...object.FormatValidatorOption) Option {
	return func(c *cfg) {
		c.fmtValidatorOpts = v
	}
}

func WithNetworkState(v netmap.State) Option {
	return func(c *cfg) {
		c.networkState = v
		c.fmtValidatorOpts = append(c.fmtValidatorOpts, object.WithNetState(v))
	}
}

func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientConstructor = v
	}
}

func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}
