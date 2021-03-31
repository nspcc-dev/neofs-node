package wrapper

import (
	reputationClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
)

type (
	// PutArgs groups the arguments of "put reputation value" invocation call.
	PutArgs struct {
		epoch  uint64
		peerID reputation.PeerID
		value  []byte // todo: replace with struct
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
func (p *PutArgs) SetValue(v []byte) {
	p.value = v
}

// Put invokes direct call of "put reputation value" method of reputation contract.
func (w *ClientWrapper) Put(v PutArgs) error {
	args := reputationClient.PutArgs{}
	args.SetEpoch(v.epoch)
	args.SetPeerID(v.peerID.Bytes())
	args.SetValue(v.value) // todo: marshal reputation value to `value` bytes there

	return (*reputationClient.Client)(w).Put(args)
}

// PutViaNotary invokes notary call of "put reputation value" method of
// reputation contract.
func (w *ClientWrapper) PutViaNotary(v PutArgs) error {
	args := reputationClient.PutArgs{}
	args.SetEpoch(v.epoch)
	args.SetPeerID(v.peerID.Bytes())
	args.SetValue(v.value) // todo: marshal reputation value to `value` bytes there

	return (*reputationClient.Client)(w).PutViaNotary(args)
}
