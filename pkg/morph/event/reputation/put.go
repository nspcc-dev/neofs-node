package reputation

import (
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-api-go/pkg/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

// Put structure of reputation.reputationPut notification from
// morph chain.
type Put struct {
	epoch  uint64
	peerID reputation.PeerID
	value  reputation.GlobalTrust
}

const peerIDLength = 33 // compressed public key

// MorphEvent implements Neo:Morph Event interface.
func (Put) MorphEvent() {}

// Epoch returns epoch value of reputation data.
func (p Put) Epoch() uint64 {
	return p.epoch
}

// PeerID returns peer id of reputation data.
func (p Put) PeerID() reputation.PeerID {
	return p.peerID
}

// Value returns reputation structure.
func (p Put) Value() reputation.GlobalTrust {
	return p.value
}

// ParsePut from notification into reputation event structure.
func ParsePut(prms []stackitem.Item) (event.Event, error) {
	var (
		ev  Put
		err error
	)

	if ln := len(prms); ln != 3 {
		return nil, event.WrongNumberOfParameters(3, ln)
	}

	// parse epoch number
	epoch, err := client.IntFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get integer epoch number")
	}

	ev.epoch = uint64(epoch)

	// parse peer ID value
	peerID, err := client.BytesFromStackItem(prms[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get peer ID value")
	}

	if ln := len(peerID); ln != peerIDLength {
		return nil, errors.Errorf("peer ID is %d byte long, expected %d", ln, peerIDLength)
	}

	var publicKey [33]byte
	copy(publicKey[:], peerID)
	ev.peerID.SetPublicKey(publicKey)

	// parse global trust value
	rawValue, err := client.BytesFromStackItem(prms[2])
	if err != nil {
		return nil, errors.Wrap(err, "could not get global trust value")
	}

	err = ev.value.Unmarshal(rawValue)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse global trust value")
	}

	return ev, nil
}
