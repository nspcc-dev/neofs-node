package timers

import (
	"context"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	localTimer struct {
		duration time.Duration
		timer    *time.Timer
		handler  event.Handler
	}

	// Timers is a component for local inner ring timers to produce local events.
	Timers struct {
		log *zap.Logger

		epoch localTimer
	}

	// Params for timers instance constructor.
	Params struct {
		Log           *zap.Logger
		EpochDuration time.Duration
	}
)

const (
	// EpochTimer is a type for HandlerInfo structure.
	EpochTimer = "EpochTimer"
)

// New creates instance of timers component.
func New(p *Params) *Timers {
	return &Timers{
		log:   p.Log,
		epoch: localTimer{duration: p.EpochDuration},
	}
}

// Start runs all available local timers.
func (t *Timers) Start(ctx context.Context) {
	t.epoch.timer = time.NewTimer(t.epoch.duration)
	go t.serve(ctx)
}

func (t *Timers) serve(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			t.log.Info("timers are getting stopped")
			t.epoch.timer.Stop()

			return
		case <-t.epoch.timer.C:
			// reset timer so it can tick once again
			t.epoch.timer.Reset(t.epoch.duration)

			// call handler if it is set
			if t.epoch.handler != nil {
				t.epoch.handler(NewEpochTick{})
			}
		}
	}
}

// RegisterHandler of local timers events.
func (t *Timers) RegisterHandler(h event.HandlerInfo) error {
	if h.Handler() == nil {
		return errors.New("ir/timers: can't register nil handler")
	}

	switch h.GetType() {
	case EpochTimer:
		t.epoch.handler = h.Handler()
	default:
		return errors.New("ir/timers: unknown handler type")
	}

	return nil
}
