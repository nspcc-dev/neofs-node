package neofs

import (
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

	err := np.netmapClient.SetConfig(config.ID(), config.Key(), config.Value())
	if err != nil {
		np.log.Error("can't relay set config event", zap.Error(err))
	}
}
