package audit

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/audit"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
)

// Report tracks the progress of auditing container data.
type Report struct {
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
	return r.res
}

// Complete completes audit report.
func (r *Report) Complete() {
	r.res.SetComplete(true)
}
