package neofs

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"go.uber.org/zap"
)

// Process update inner ring event by setting inner ring list value from
// main chain in side chain.
func (np *Processor) processUpdateInnerRing(list *neofsEvent.UpdateInnerRing) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore inner ring update")
		return
	}

	err := invoke.UpdateInnerRing(np.morphClient, np.netmapContract,
		list.Keys(),
	)
	if err != nil {
		np.log.Error("can't relay update inner ring event", zap.Error(err))
	}
}
