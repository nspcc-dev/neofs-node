package internal

// Error is a custom error.
type Error string

// Error is an implementation of error interface.
func (e Error) Error() string { return string(e) }
