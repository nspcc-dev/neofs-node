package audit

import (
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/audit"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

// Report tracks the progress of auditing container data.
type Report struct {
	mu  sync.RWMutex
	res *audit.Result
}

// Reporter is an interface of the entity that records
// the data audit report.
type Reporter interface {
	WriteReport(r *Report) error
}

// NewReport creates and returns blank Report instance.
func NewReport(cid *container.ID) *Report {
	rep := &Report{
		res: audit.NewResult(),
	}

	rep.res.SetContainerID(cid)

	return rep
}

// Result forms the structure of the data audit result.
func (r *Report) Result() *audit.Result {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.res
}

// Complete completes audit report.
func (r *Report) Complete() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.SetComplete(true)
}

// PassedPoR updates list of passed storage groups.
func (r *Report) PassedPoR(sg *object.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.SetPassSG(append(r.res.PassSG(), sg))
}

// FailedPoR updates list of failed storage groups.
func (r *Report) FailedPoR(sg *object.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.SetFailSG(append(r.res.FailSG(), sg))
}

// SetPlacementCounters sets counters of compliance with placement.
func (r *Report) SetPlacementCounters(hit, miss, fail uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.SetHit(hit)
	r.res.SetMiss(miss)
	r.res.SetFail(fail)
}

// SetPDPResults sets lists of nodes according to their PDP results.
func (r *Report) SetPDPResults(passed, failed [][]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.SetPassNodes(passed)
	r.res.SetFailNodes(failed)
}

// SetPoRCounters sets amounts of head requests and retries at PoR audit stage.
func (r *Report) SetPoRCounters(requests, retries uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.res.SetRequests(requests)
	r.res.SetRetries(retries)
}
