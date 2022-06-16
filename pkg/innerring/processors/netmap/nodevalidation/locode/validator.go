package locode

// Prm groups the required parameters of the Validator's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// NeoFS LOCODE database interface.
	//
	// Must not be nil.
	DB DB
}

// Validator is a utility that verifies and updates
// node attributes associated with its geographical location
// (LOCODE).
//
// For correct operation, the Validator must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the Validator is immediately ready to work through API.
type Validator struct {
	db DB
}

// New creates a new instance of the Validator.
//
// Panics if at least one value of the parameters is invalid.
//
// The created Validator does not require additional
// initialization and is completely ready for work.
func New(prm Prm) *Validator {
	return &Validator{
		db: prm.DB,
	}
}
