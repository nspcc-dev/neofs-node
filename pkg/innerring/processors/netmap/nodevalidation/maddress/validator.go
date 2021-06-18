package maddress

// Validator is an utility that verifies node
// multiaddress.
//
// For correct operation, Validator must be created
// using the constructor (New). After successful creation,
// the Validator is immediately ready to work through API.
type Validator struct {}

// New creates a new instance of the Validator.
//
// The created Validator does not require additional
// initialization and is completely ready for work.
func New() *Validator {
	return &Validator{}
}
