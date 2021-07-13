package intermediate

import (
	"encoding/hex"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	eigentrustcalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	consumerstorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/consumers"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/daughters"
)

// DaughterTrustIteratorProvider is implementation of the
// reputation/eigentrust/calculator's DaughterTrustIteratorProvider interface.
type DaughterTrustIteratorProvider struct {
	DaughterStorage *daughters.Storage
	ConsumerStorage *consumerstorage.Storage
}

type ErrNoData struct {
	hasDaughter bool
	daughter    reputation.PeerID
	epoch       uint64
}

func (e *ErrNoData) Error() string {
	if e.hasDaughter {
		return fmt.Sprintf("no data in %d epoch for peer: %s", e.epoch, hex.EncodeToString(e.daughter.Bytes()))
	}

	return fmt.Sprintf("no daughter data in %d epoch", e.epoch)
}

// InitDaughterIterator returns iterator over received
// local trusts for ctx.Epoch() epoch from daughter p.
//
// Returns ErrNoData if there is no trust data for
// specified epoch and daughter's PeerId.
func (ip *DaughterTrustIteratorProvider) InitDaughterIterator(ctx eigentrustcalc.Context,
	p reputation.PeerID) (eigentrustcalc.TrustIterator, error) {
	daughterIterator, ok := ip.DaughterStorage.DaughterTrusts(ctx.Epoch(), p)
	if !ok {
		return nil, &ErrNoData{
			daughter:    p,
			hasDaughter: true,
			epoch:       ctx.Epoch(),
		}
	}

	return daughterIterator, nil
}

// InitAllDaughtersIterator returns iterator over all
// daughters of the current node(manager) and all local
// trusts received from them for ctx.Epoch() epoch.
//
// Returns ErrNoData if there is no trust data for
// specified epoch.
func (ip *DaughterTrustIteratorProvider) InitAllDaughtersIterator(
	ctx eigentrustcalc.Context) (eigentrustcalc.PeerTrustsIterator, error) {
	iter, ok := ip.DaughterStorage.AllDaughterTrusts(ctx.Epoch())
	if !ok {
		return nil, &ErrNoData{epoch: ctx.Epoch()}
	}

	return iter, nil
}

// InitConsumersIterator returns iterator over all daughters
// of the current node(manager) and all their consumers' local
// trusts for ctx.Epoch() epoch and ctx.I() iteration.
//
// Returns ErrNoData if there is no trust data for
// specified epoch and iteration.
func (ip *DaughterTrustIteratorProvider) InitConsumersIterator(
	ctx eigentrustcalc.Context) (eigentrustcalc.PeerTrustsIterator, error) {
	consumerIterator, ok := ip.ConsumerStorage.Consumers(ctx.Epoch(), ctx.I())
	if !ok {
		return nil, &ErrNoData{epoch: ctx.Epoch()}
	}

	return consumerIterator, nil
}
