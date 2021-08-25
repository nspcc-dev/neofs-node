package audittask

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit/auditor"
	"go.uber.org/zap"
)

// Listen starts the process of processing tasks from the queue.
//
// The listener is terminated by context.
func (m *Manager) Listen(ctx context.Context) {
	m.log.Info("process routine",
		zap.Uint32("queue_capacity", m.queueCap),
	)

	m.ch = make(chan *audit.Task, m.queueCap)

	for {
		select {
		case <-ctx.Done():
			m.log.Warn("stop listener by context",
				zap.String("error", ctx.Err().Error()),
			)

			return
		case task, ok := <-m.ch:
			if !ok {
				m.log.Warn("queue channel is closed")
				return
			}

			m.handleTask(task)
		}
	}
}

func (m *Manager) handleTask(task *audit.Task) {
	pdpPool, err := m.pdpPoolGenerator()
	if err != nil {
		m.log.Error("could not generate PDP worker pool",
			zap.String("error", err.Error()),
		)

		return
	}

	porPool, err := m.pdpPoolGenerator()
	if err != nil {
		m.log.Error("could not generate PoR worker pool",
			zap.String("error", err.Error()),
		)

		return
	}

	auditContext := m.generateContext(task).
		WithPDPWorkerPool(pdpPool).
		WithPoRWorkerPool(porPool)

	if err := m.workerPool.Submit(auditContext.Execute); err != nil {
		// may be we should report it
		m.log.Warn("could not submit audit task")
	}
}

func (m *Manager) generateContext(task *audit.Task) *auditor.Context {
	return auditor.NewContext(m.ctxPrm).
		WithTask(task)
}
