package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/reputation"
	reputationClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
)

type (
	// PutArgs groups the arguments of "put reputation value" invocation call.
	PutArgs struct {
		epoch  uint64
		peerID reputation.PeerID
		value  reputation.GlobalTrust
	}
)

// SetEpoch sets epoch of reputation value.
func (p *PutArgs) SetEpoch(v uint64) {
	p.epoch = v
}

// SetPeerID sets peer id of reputation value.
func (p *PutArgs) SetPeerID(v reputation.PeerID) {
	p.peerID = v
}

// SetValue sets reputation value.
func (p *PutArgs) SetValue(v reputation.GlobalTrust) {
	p.value = v
}

// Put invokes direct call of "put reputation value" method of reputation contract.
//
// If TryNotary is provided, calls notary contract.
func (w *ClientWrapper) Put(v PutArgs) error {
	args, err := preparePutArgs(v)
	if err != nil {
		return err
	}

	return (*reputationClient.Client)(w).Put(args)
}

// PutViaNotary invokes notary call of "put reputation value" method of
// reputation contract.
func (w *ClientWrapper) PutViaNotary(v PutArgs) error {
	args, err := preparePutArgs(v)
	if err != nil {
		return err
	}

	return (*reputationClient.Client)(w).PutViaNotary(args)
}

func preparePutArgs(v PutArgs) (reputationClient.PutArgs, error) {
	args := reputationClient.PutArgs{}

	data, err := v.value.Marshal()
	if err != nil {
		return args, fmt.Errorf("can't marshal global trust value: %w", err)
	}

	args.SetEpoch(v.epoch)
	args.SetPeerID(v.peerID.ToV2().GetPublicKey())
	args.SetValue(data)

	return args, nil
}
