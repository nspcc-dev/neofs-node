package innerring

// EpochCounter is a getter for a global epoch counter.
func (s *Server) EpochCounter() uint64 {
	return s.epochCounter.Load()
}

// SetEpochCounter is a setter for contract processors to update global
// epoch counter.
func (s *Server) SetEpochCounter(val uint64) {
	s.epochCounter.Store(val)
}

// IsActive is a getter for a global active flag state.
func (s *Server) IsActive() bool {
	return s.innerRingIndex.Load() >= 0
}

// Index is a getter for a global index of node in inner ring list. Negative
// index means that node is not in the inner ring list.
func (s *Server) Index() int32 {
	return s.innerRingIndex.Load()
}
