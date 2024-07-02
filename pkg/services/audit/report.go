package audit

import (
	"sync"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Report tracks the progress of auditing container data.
type Report struct {
	mu  sync.RWMutex
	res Result
}

// Reporter is an interface of the entity that records
// the data audit report.
type Reporter interface {
	WriteReport(r *Report) error
}

// NewReport creates and returns blank Report instance.
func NewReport(cnr cid.ID) *Report {
	return &Report{res: Result{Container: cnr}}
}

// Result forms the structure of the data audit result.
func (r *Report) Result() *Result {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &r.res
}

// Complete completes audit report.
func (r *Report) Complete() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.Completed = true
}

// PassedPoR updates list of passed storage groups.
func (r *Report) PassedPoR(sg oid.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.PoR.PassedStorageGroups = append(r.res.PoR.PassedStorageGroups, sg)
}

// FailedPoR updates list of failed storage groups.
func (r *Report) FailedPoR(sg oid.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.PoR.FailedStorageGroups = append(r.res.PoR.FailedStorageGroups, sg)
}

// SetPlacementCounters sets counters of compliance with placement.
func (r *Report) SetPlacementCounters(hit, miss, fail uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.PoP.Hits = hit
	r.res.PoP.Misses = miss
	r.res.PoP.Failures = fail
}

// SetPDPResults sets lists of nodes according to their PDP results.
func (r *Report) SetPDPResults(passed, failed [][]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.PDP.PassedStorageNodes = passed
	r.res.PDP.FailedStorageNodes = failed
}

// SetPoRCounters sets amounts of head requests and retries at PoR audit stage.
func (r *Report) SetPoRCounters(requests, retries uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.PoR.Requests = requests
	r.res.PoR.Retries = retries
}
