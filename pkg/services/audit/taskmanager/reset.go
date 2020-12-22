package audittask

// Reset pops all tasks from the queue.
// Returns amount of popped elements.
func (m *Manager) Reset() (popped int) {
	for ; len(m.ch) > 0; popped++ {
		<-m.ch
	}

	return
}
