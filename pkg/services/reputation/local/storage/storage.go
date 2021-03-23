package truststorage

import (
	"sync"
)

// Prm groups the required parameters of the Storage's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
//
// The component is not parameterizable at the moment.
type Prm struct{}

// Storage represents in-memory storage of
// local reputation values.
//
// Storage provides access to normalized local trust
// values through iterator interface.
//
// For correct operation, Storage must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// Storage is immediately ready to work through API.
type Storage struct {
	prm Prm

	mtx sync.RWMutex

	mItems map[uint64]*EpochTrustValueStorage
}

// New creates a new instance of the Storage.
//
// The created Storage does not require additional
// initialization and is completely ready for work.
func New(prm Prm) *Storage {
	return &Storage{
		prm:    prm,
		mItems: make(map[uint64]*EpochTrustValueStorage),
	}
}
