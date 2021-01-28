package settlement

import "github.com/nspcc-dev/neofs-node/pkg/util/logger"

type auditEventHandler struct {
	log *logger.Logger

	epoch uint64

	proc AuditProcessor
}

func (p *auditEventHandler) handle() {
	p.log.Info("process audit settlements")

	p.proc.ProcessAuditSettlements(p.epoch)

	p.log.Info("audit processing finished")
}
