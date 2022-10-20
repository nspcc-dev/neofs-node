package neofs

import (
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// Process config event by setting configuration value from the mainchain in
// the sidechain.
func (np *Processor) processConfig(config *neofsEvent.Config) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore config")
		return
	}

	prm := nmClient.SetConfigPrm{}

	prm.SetID(config.ID())
	prm.SetKey(config.Key())
	prm.SetValue(config.Value())
	prm.SetHash(config.TxHash())

	err := np.netmapClient.SetConfig(prm)
	if err != nil {
		np.log.Error("can't relay set config event", logger.FieldError(err))
	}
}
