package workers

import (
	"context"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/modules/fix/worker"
	"github.com/spf13/viper"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type (
	// Result returns wrapped workers group for DI.
	Result struct {
		dig.Out

		Workers []*worker.Job
	}

	// Params is dependencies for create workers slice.
	Params struct {
		dig.In

		Jobs   worker.Jobs
		Viper  *viper.Viper
		Logger *zap.Logger
	}
)

func prepare(p Params) worker.Workers {
	w := worker.New()

	for name, handler := range p.Jobs {
		if job := byConfig(name, handler, p.Logger, p.Viper); job != nil {
			p.Logger.Debug("worker: add new job",
				zap.String("name", name))

			w.Add(job)
		}
	}

	return w
}

func byTicker(d time.Duration, h worker.Handler) worker.Handler {
	return func(ctx context.Context) {
		ticker := time.NewTicker(d)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					h(ctx)
				}
			}
		}
	}
}

func byTimer(d time.Duration, h worker.Handler) worker.Handler {
	return func(ctx context.Context) {
		timer := time.NewTimer(d)
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					h(ctx)
					timer.Reset(d)
				}
			}
		}
	}
}

func byConfig(name string, h worker.Handler, l *zap.Logger, v *viper.Viper) worker.Handler {
	var job worker.Handler

	if !v.IsSet("workers." + name) {
		l.Info("worker: has no configuration",
			zap.String("worker", name))
		return nil
	}

	if v.GetBool("workers." + name + ".disabled") {
		l.Info("worker: disabled",
			zap.String("worker", name))
		return nil
	}

	if ticker := v.GetDuration("workers." + name + ".ticker"); ticker > 0 {
		job = byTicker(ticker, h)
	}

	if timer := v.GetDuration("workers." + name + ".timer"); timer > 0 {
		job = byTimer(timer, h)
	}

	if v.GetBool("workers." + name + ".immediately") {
		return func(ctx context.Context) {
			h(ctx)

			if job == nil {
				return
			}

			// check context before run immediately job again
			select {
			case <-ctx.Done():
				return
			default:
			}

			job(ctx)
		}
	}

	return job
}
