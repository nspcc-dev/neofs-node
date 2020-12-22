package audittask

// Reset pops all tasks from the queue.
func (m *Manager) Reset() {
	for len(m.ch) > 0 {
		<-m.ch
	}
}
