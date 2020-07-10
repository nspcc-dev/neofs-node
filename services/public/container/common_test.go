package container

// Entity for mocking interfaces.
// Implementation of any interface intercepts arguments via f (if not nil).
// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
type testCommonEntity struct {
	// Set of interfaces which entity must implement, but some methods from those does not call.

	// Argument interceptor. Used for ascertain of correct parameter passage between components.
	f func(...interface{})
	// Mocked result of any interface.
	res interface{}
	// Mocked error of any interface.
	err error
}

func (s testCommonEntity) Healthy() error {
	return s.err
}
