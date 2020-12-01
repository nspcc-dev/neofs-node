package gc

import (
	"context"
	"time"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// GC represents an object garbage collector.
type GC struct {
	*cfg

	timer *time.Timer

	queue chan *object.Address
}

// Option represents GC constructor option.
type Option func(*cfg)

type cfg struct {
	sleepInterval, workInterval time.Duration

	queueCap uint32

	log *logger.Logger

	remover Remover
}

// Remover is an interface of the component that stores objects.
type Remover interface {
	// Delete removes object from physical storage.
	Delete(...*object.Address) error
}

func defaultCfg() *cfg {
	return &cfg{
		sleepInterval: 5 * time.Second,
		workInterval:  5 * time.Second,
		queueCap:      10,
		log:           zap.L(),
	}
}

// New creates, initializes and returns GC instance.
func New(opts ...Option) *GC {
	cfg := defaultCfg()

	for i := range opts {
		opts[i](cfg)
	}

	cfg.log = cfg.log.With(zap.String("component", "Object GC"))

	return &GC{
		cfg: cfg,
	}
}

func (gc *GC) Run(ctx context.Context) {
	defer func() {
		close(gc.queue)
		gc.timer.Stop()
		gc.log.Info("routine stopped")
	}()

	gc.log.Info("process routine",
		zap.Uint32("queue capacity", gc.queueCap),
		zap.Duration("sleep interval", gc.sleepInterval),
		zap.Duration("working interval", gc.workInterval),
	)

	gc.queue = make(chan *object.Address, gc.queueCap)
	gc.timer = time.NewTimer(gc.sleepInterval)

	for {
		select {
		case <-ctx.Done():
			gc.log.Warn("context is done",
				zap.String("error", ctx.Err().Error()),
			)

			return
		case _, ok := <-gc.timer.C:
			if !ok {
				gc.log.Warn("timer is stopped")

				return
			}

			abort := time.After(gc.workInterval)

		loop:
			for {
				select {
				case <-ctx.Done():
					gc.log.Warn("context is done",
						zap.String("error", ctx.Err().Error()),
					)

					return
				case <-abort:
					break loop
				case addr, ok := <-gc.queue:
					if !ok {
						gc.log.Warn("queue channel is closed")
					} else if err := gc.remover.Delete(addr); err != nil {
						gc.log.Error("could not remove object",
							zap.String("error", err.Error()),
						)
					} else {
						gc.log.Info("object removed",
							zap.String("CID", stringifyCID(addr.ContainerID())),
							zap.String("ID", stringifyID(addr.ObjectID())),
						)
					}
				}
			}

			gc.timer.Reset(gc.sleepInterval)
		}
	}
}

func stringifyID(addr *object.ID) string {
	return base58.Encode(addr.ToV2().GetValue())
}

func stringifyCID(addr *container.ID) string {
	return base58.Encode(addr.ToV2().GetValue())
}

// DeleteObjects adds list of adresses to delete queue.
func (gc *GC) DeleteObjects(list ...*object.Address) {
	for i := range list {
		select {
		case gc.queue <- list[i]:
		default:
			gc.log.Info("queue for deletion is full")
		}
	}
}
