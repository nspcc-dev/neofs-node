package neofs

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"go.uber.org/zap"
)

// Process config event by setting configuration value from main chain in
// side chain.
func (np *Processor) processConfig(config *neofsEvent.Config) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore config")
		return
	}

	prm := wrapper.SetConfigPrm{}

	prm.SetID(config.ID())
	prm.SetKey(config.Key())
	prm.SetValue(config.Value())
	prm.SetHash(config.TxHash())

	err := np.netmapClient.SetConfig(prm)
	if err != nil {
		np.log.Error("can't relay set config event", zap.Error(err))
	}
}
