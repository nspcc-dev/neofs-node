package loadcontroller

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
	// Iterator over the used space values of the containers
	// collected by the node locally.
	LocalMetrics IteratorProvider

	// Place of recording the local values of
	// the used space of containers.
	LocalAnnouncementTarget WriterProvider

	// Iterator over the summarized used space scores
	// from the various network participants.
	AnnouncementAccumulator IteratorProvider

	// Place of recording the final estimates of
	// the used space of containers.
	ResultReceiver WriterProvider
}

// Controller represents main handler for starting
// and interrupting container volume estimation.
//
// It binds the interfaces of the local value stores
// to the target storage points. Controller is abstracted
// from the internal storage device and the network location
// of the connecting components. At its core, it is a
// high-level start-stop trigger for calculations.
//
// For correct operation, the controller must be created
// using the constructor (New) based on the required parameters
// and optional components. After successful creation,
// the constructor is immediately ready to work through
// API of external control of calculations and data transfer.
type Controller struct {
	prm Prm

	opts *options

	announceMtx  sync.Mutex
	mAnnounceCtx map[uint64]context.CancelFunc

	reportMtx  sync.Mutex
	mReportCtx map[uint64]context.CancelFunc
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
	case prm.LocalMetrics == nil:
		panicOnPrmValue("LocalMetrics", prm.LocalMetrics)
	case prm.AnnouncementAccumulator == nil:
		panicOnPrmValue("AnnouncementAccumulator", prm.AnnouncementAccumulator)
	case prm.LocalAnnouncementTarget == nil:
		panicOnPrmValue("LocalAnnouncementTarget", prm.LocalAnnouncementTarget)
	case prm.ResultReceiver == nil:
		panicOnPrmValue("ResultReceiver", prm.ResultReceiver)
	}

	o := defaultOpts()

	for _, opt := range opts {
		opt(o)
	}

	return &Controller{
		prm:          prm,
		opts:         o,
		mAnnounceCtx: make(map[uint64]context.CancelFunc),
		mReportCtx:   make(map[uint64]context.CancelFunc),
	}
}
