package alphabet

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"go.uber.org/zap"
)

func (np *Processor) processEmit() {
	index := np.irList.Index()
	if index < 0 {
		np.log.Info("passive mode, ignore gas emission event")
		return
	} else if int(index) >= len(np.alphabetContracts) {
		np.log.Debug("node is out of alphabet range, ignore gas emission event",
			zap.Int32("index", index))
		return
	}

	err := invoke.AlphabetEmit(np.morphClient, np.alphabetContracts[index])
	if err != nil {
		np.log.Warn("can't invoke alphabet emit method")
	}
}
