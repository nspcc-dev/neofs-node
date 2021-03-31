package wrapper

import (
	reputationClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
)

type (
	// GetArgs groups the arguments of "get reputation value" test invocation.
	GetArgs struct {
		epoch  uint64
		peerID reputation.PeerID
	}

	// GetByIDArgs groups the arguments of "get reputation value by
	// reputation id" test invocation.
	GetByIDArgs struct {
		id ReputationID
	}

	// GetResults groups the result of "get reputation value" and
	// "get reputation value by reputation id" test invocations.
	GetResult struct {
		reputations [][]byte // todo: replace with the slice of structures
	}
)

// SetEpoch sets epoch of expected reputation value.
func (g *GetArgs) SetEpoch(v uint64) {
	g.epoch = v
}

// SetPeerID sets peer id of expected reputation value.
func (g *GetArgs) SetPeerID(v reputation.PeerID) {
	g.peerID = v
}

// SetID sets id of expected reputation value in reputation contract.
func (g *GetByIDArgs) SetID(v ReputationID) {
	g.id = v
}

// Reputations returns slice of reputation values.
func (g GetResult) Reputations() [][]byte {
	return g.reputations
}

// Get invokes the call of "get reputation value" method of reputation contract.
func (w *ClientWrapper) Get(v GetArgs) (*GetResult, error) {
	args := reputationClient.GetArgs{}
	args.SetEpoch(v.epoch)
	args.SetPeerID(v.peerID.Bytes())

	data, err := (*reputationClient.Client)(w).Get(args)
	if err != nil {
		return nil, err
	}

	return parseGetResult(data)
}

// GetByID invokes the call of "get reputation value by reputation id" method
// of reputation contract.
func (w *ClientWrapper) GetByID(v GetByIDArgs) (*GetResult, error) {
	args := reputationClient.GetByIDArgs{}
	args.SetID(v.id)

	data, err := (*reputationClient.Client)(w).GetByID(args)
	if err != nil {
		return nil, err
	}

	return parseGetResult(data)
}

func parseGetResult(data *reputationClient.GetResult) (*GetResult, error) {
	rawReputations := data.Reputations()

	// todo: unmarshal all reputation values into structure

	return &GetResult{
		reputations: rawReputations,
	}, nil
}
