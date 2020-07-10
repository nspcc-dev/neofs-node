package peers

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/connectivity"
)

func (s *iface) Job(ctx context.Context) {
	var (
		tick    = time.NewTimer(s.tick)
		metrics = time.NewTimer(s.metricsTimeout)
	)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-metrics.C:
			var items = make(map[connectivity.State]float64)
			s.grpc.globalMutex.Lock()
			for _, item := range s.grpc.connBook {
				if item.conn != nil {
					items[item.conn.GetState()]++
				}
			}
			s.grpc.globalMutex.Unlock()

			updateMetrics(items)

			metrics.Reset(s.metricsTimeout)
		case <-tick.C:
			var count int

			s.grpc.globalMutex.Lock()
			for addr, item := range s.grpc.connBook {
				if item.conn == nil || isGRPCClosed(item.conn) || time.Since(item.used) > s.idle {
					if err := s.removeGRPCConnection(addr); err != nil {
						s.log.Error("could not close connection",
							zap.String("address", addr),
							zap.String("target", item.conn.Target()),
							zap.Stringer("idle", time.Since(item.used)),
							zap.Error(err))
						continue
					}

					count++
				} else {
					s.log.Debug("ignore connection",
						zap.String("address", addr),
						zap.Stringer("idle", time.Since(item.used)))
				}
			}
			s.grpc.globalMutex.Unlock()

			s.log.Debug("cleanup connections done",
				zap.Int("closed", count))

			tick.Reset(s.tick)
		}
	}

	tick.Stop()
}
