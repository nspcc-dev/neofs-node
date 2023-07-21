package settlement

import "go.uber.org/zap"

type auditEventHandler struct {
	log *zap.Logger

	epoch uint64

	proc AuditProcessor
}

func (p *auditEventHandler) handle() {
	p.log.Info("process audit settlements")

	p.proc.ProcessAuditSettlements(p.epoch)

	p.log.Info("audit processing finished")
}
