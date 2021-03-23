package neofs

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
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

	err := invoke.SetConfig(np.morphClient, np.netmapContract,
		&invoke.SetConfigArgs{
			ID:    config.ID(),
			Key:   config.Key(),
			Value: config.Value(),
		},
	)
	if err != nil {
		np.log.Error("can't relay set config event", zap.Error(err))
	}
}
