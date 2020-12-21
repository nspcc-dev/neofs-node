package audit

import (
	auditAPI "github.com/nspcc-dev/neofs-api-go/pkg/audit"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	"github.com/pkg/errors"
)

// PutAuditResult saves passed audit result structure in NeoFS system
// through Audit contract call.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
func (w *ClientWrapper) PutAuditResult(result *auditAPI.Result) error {
	rawResult, err := result.Marshal()
	if err != nil {
		return errors.Wrap(err, "could not marshal audit result")
	}

	args := audit.PutAuditResultArgs{}
	args.SetRawResult(rawResult)

	return (*audit.Client)(w).
		PutAuditResult(args)
}

// ListAuditResults returns a list of all audit results in NeoFS system.
// The list is composed through Audit contract call.
func (w *ClientWrapper) ListAuditResults() ([]*auditAPI.Result, error) {
	args := audit.ListResultsArgs{}

	values, err := (*audit.Client)(w).ListAuditResults(args)
	if err != nil {
		return nil, err
	}

	rawResults := values.RawResults()
	result := make([]*auditAPI.Result, 0, len(rawResults))

	for i := range rawResults {
		auditRes := auditAPI.NewResult()
		if err := auditRes.Unmarshal(rawResults[i]); err != nil {
			return nil, errors.Wrap(err, "could not unmarshal audit result structure")
		}

		result = append(result, auditRes)
	}

	return result, nil
}
