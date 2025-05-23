package errors

// Temporary marks cause error as temporary.
type Temporary struct {
	Cause error
}

// Error implements built-in error interface.
func (x Temporary) Error() string {
	return x.Cause.Error()
}

// Unwrap implements interface used by std errors package functions.
func (x Temporary) Unwrap() error {
	return x.Cause
}
