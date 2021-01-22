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

		alphabet localTimer
	}

	// Params for timers instance constructor.
	Params struct {
		Log              *zap.Logger
		EpochDuration    time.Duration
		AlphabetDuration time.Duration
	}
)

const (
	// AlphabetTimer is a type for HandlerInfo structure.
	AlphabetTimer = "AlphabetTimer"
)

// New creates instance of timers component.
func New(p *Params) *Timers {
	return &Timers{
		log:      p.Log,
		alphabet: localTimer{duration: p.AlphabetDuration},
	}
}

// Start runs all available local timers.
func (t *Timers) Start(ctx context.Context) {
	t.alphabet.timer = time.NewTimer(t.alphabet.duration)
	go t.serve(ctx)
}

func (t *Timers) serve(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			t.log.Info("timers are getting stopped")
			t.alphabet.timer.Stop()

			return
		case <-t.alphabet.timer.C:
			// reset timer so it can tick once again
			t.alphabet.timer.Reset(t.alphabet.duration)
			// call handler, it should be always set
			t.alphabet.handler(NewAlphabetEmitTick{})
		}
	}
}

// RegisterHandler of local timers events.
func (t *Timers) RegisterHandler(h event.HandlerInfo) error {
	if h.Handler() == nil {
		return errors.New("ir/timers: can't register nil handler")
	}

	switch h.GetType() {
	case AlphabetTimer:
		t.alphabet.handler = h.Handler()
	default:
		return errors.New("ir/timers: unknown handler type")
	}

	return nil
}
