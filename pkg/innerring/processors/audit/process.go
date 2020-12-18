package audit

import (
	"go.uber.org/zap"
)

func (ap *Processor) processStartAudit(epoch uint64) {
	// todo: flush left audit results from audit result cache

	containers, err := ap.selectContainersToAudit(epoch)
	if err != nil {
		ap.log.Error("container selection failure",
			zap.String("error", err.Error()))

		return
	}

	ap.log.Info("select containers for audit", zap.Int("amount", len(containers)))

	// todo: for each container get list of storage groups
	// todo: for each container create audit result template in audit result cache
	// todo: for each container push audit tasks into queue
}
