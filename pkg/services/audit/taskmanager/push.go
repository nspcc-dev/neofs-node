package audittask

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
)

// PushTask adds a task to the queue for processing.
//
// Returns error if task was not added to the queue.
func (m *Manager) PushTask(t *audit.Task) error {
	m.ch <- t
	return nil
}
