package audit

import (
	auditAPI "github.com/nspcc-dev/neofs-api-go/pkg/audit"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	"github.com/pkg/errors"
)

// ResultID is an identity of audit result inside audit contract.
type ResultID []byte

var errUnsupported = errors.New("unsupported structure version")

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

// ListAuditResults returns a list of all audit result IDs inside audit contract.
func (w *ClientWrapper) ListAllAuditResultID() ([]ResultID, error) {
	args := audit.ListResultsArgs{}

	values, err := (*audit.Client)(w).ListAuditResults(args)
	if err != nil {
		return nil, err
	}

	return parseRawResult(values), nil
}

// ListAuditResultIDByEpoch returns a list of audit result IDs inside audit
// contract for specific epoch number.
func (w *ClientWrapper) ListAuditResultIDByEpoch(epoch uint64) ([]ResultID, error) {
	args := audit.ListResultsByEpochArgs{}
	args.SetEpoch(int64(epoch))

	values, err := (*audit.Client)(w).ListAuditResultsByEpoch(args)
	if err != nil {
		return nil, err
	}

	return parseRawResult(values), nil
}

// ListAuditResultIDByCID returns a list of audit result IDs inside audit
// contract for specific epoch number and container ID.
func (w *ClientWrapper) ListAuditResultIDByCID(epoch uint64, cid *container.ID) ([]ResultID, error) {
	args := audit.ListResultsByCIDArgs{}
	args.SetEpoch(int64(epoch))

	v2 := cid.ToV2()
	if v2 == nil {
		return nil, errUnsupported // use other major version if there any
	}

	args.SetCID(v2.GetValue())

	values, err := (*audit.Client)(w).ListAuditResultsByCID(args)
	if err != nil {
		return nil, err
	}

	return parseRawResult(values), nil
}

// ListAuditResultIDByNode returns a list of audit result IDs inside audit
// contract for specific epoch number, container ID and inner ring public key.
func (w *ClientWrapper) ListAuditResultIDByNode(epoch uint64, cid *container.ID, key []byte) ([]ResultID, error) {
	args := audit.ListResultsByNodeArgs{}
	args.SetEpoch(int64(epoch))
	args.SetNodeKey(key)

	v2 := cid.ToV2()
	if v2 == nil {
		return nil, errUnsupported // use other major version if there any
	}

	args.SetCID(v2.GetValue())

	values, err := (*audit.Client)(w).ListAuditResultsByNode(args)
	if err != nil {
		return nil, err
	}

	return parseRawResult(values), nil
}

func parseRawResult(values *audit.ListResultsValues) []ResultID {
	rawResults := values.RawResults()
	result := make([]ResultID, 0, len(rawResults))

	for i := range rawResults {
		result = append(result, rawResults[i])
	}

	return result
}

// GetAuditResult returns audit result structure stored in audit contract.
func (w *ClientWrapper) GetAuditResult(id ResultID) (*auditAPI.Result, error) {
	args := audit.GetAuditResultArgs{}
	args.SetID(id)

	value, err := (*audit.Client)(w).GetAuditResult(args)
	if err != nil {
		return nil, err
	}

	auditRes := auditAPI.NewResult()
	if err := auditRes.Unmarshal(value.Result()); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal audit result structure")
	}

	return auditRes, nil
}
