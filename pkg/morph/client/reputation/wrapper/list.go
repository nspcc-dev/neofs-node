package wrapper

import (
	reputationClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
)

type (
	// ReputationID is an ID of the reputation record in reputation contract.
	ReputationID []byte

	// ListByEpochArgs groups the arguments of
	// "list reputation ids by epoch" test invoke call.
	ListByEpochArgs struct {
		epoch uint64
	}

	// ListByEpochResult groups the result of "list reputation ids by epoch"
	// test invoke.
	ListByEpochResult struct {
		ids []ReputationID
	}
)

// SetEpoch sets epoch of expected reputation ids.
func (l *ListByEpochArgs) SetEpoch(v uint64) {
	l.epoch = v
}

// IDs returns slice of reputation id values.
func (l ListByEpochResult) IDs() []ReputationID {
	return l.ids
}

// ListByEpoch invokes the call of "list reputation ids by epoch" method of
// reputation contract.
func (w *ClientWrapper) ListByEpoch(v ListByEpochArgs) (*ListByEpochResult, error) {
	args := reputationClient.ListByEpochArgs{}
	args.SetEpoch(v.epoch)

	data, err := (*reputationClient.Client)(w).ListByEpoch(args)
	if err != nil {
		return nil, err
	}

	ids := data.IDs()

	result := make([]ReputationID, 0, len(ids))
	for i := range ids {
		result = append(result, ids[i])
	}

	return &ListByEpochResult{
		ids: result,
	}, nil
}
