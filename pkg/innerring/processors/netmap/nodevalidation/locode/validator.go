package locode

import (
	"github.com/nspcc-dev/locode-db/pkg/locodedb"
)

// Validator is a utility that verifies and updates
// node attributes associated with its geographical location
// (LOCODE).
//
// For correct operation, the Validator must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the Validator is immediately ready to work through API.
type Validator struct {
}

// New creates a new instance of the Validator.
//
// The created Validator does not require additional
// initialization and is completely ready for work.
func New() *Validator {
	return &Validator{}
}

func getRecord(lc string) (locodedb.Record, error) {
	rec, err := locodedb.Get(lc)
	return rec, err
}
