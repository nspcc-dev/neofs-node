package trustcontroller

import (
	"context"
	"fmt"
	"sync"
)

// Prm groups the required parameters of the Controller's constructor.
//
// All values must comply with the requirements imposed on them.
// Passing incorrect parameter values will result in constructor
// failure (error or panic depending on the implementation).
type Prm struct {
	// Iterator over the reputation values
	// collected by the node locally.
	//
	// Must not be nil.
	LocalTrustSource IteratorProvider

	// Place of recording the local values of
	// trust to other nodes.
	//
	// Must not be nil.
	LocalTrustTarget WriterProvider
}

// Controller represents main handler for starting
// and interrupting the reporting local trust values.
//
// It binds the interfaces of the local value stores
// to the target storage points. Controller is abstracted
// from the internal storage device and the network location
// of the connecting components. At its core, it is a
// high-level start-stop trigger for reporting.
//
// For correct operation, the controller must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the constructor is immediately ready to work through
// API of external control of calculations and data transfer.
type Controller struct {
	prm Prm

	opts *options

	mtx  sync.Mutex
	mCtx map[uint64]context.CancelFunc
}

const invalidPrmValFmt = "invalid parameter %s (%T):%v"

func panicOnPrmValue(n string, v interface{}) {
	panic(fmt.Sprintf(invalidPrmValFmt, n, v, v))
}

// New creates a new instance of the Controller.
//
// Panics if at least one value of the parameters is invalid.
//
// The created Controller does not require additional
// initialization and is completely ready for work
func New(prm Prm, opts ...Option) *Controller {
	switch {
	case prm.LocalTrustSource == nil:
		panicOnPrmValue("LocalTrustSource", prm.LocalTrustSource)
	case prm.LocalTrustTarget == nil:
		panicOnPrmValue("LocalTrustTarget", prm.LocalTrustTarget)
	}

	o := defaultOpts()

	for _, opt := range opts {
		opt(o)
	}

	return &Controller{
		prm:  prm,
		opts: o,
		mCtx: make(map[uint64]context.CancelFunc),
	}
}
