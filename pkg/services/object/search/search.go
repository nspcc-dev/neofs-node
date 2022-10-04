package searchsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// Search serves a request to select the objects.
func (s *Service) Search(ctx context.Context, prm Prm) error {
	exec := &execCtx{
		svc: s,
		ctx: ctx,
		prm: prm,
	}

	exec.prepare()

	exec.setLogger(s.log)

	exec.execute()

	return exec.statusError.err
}

func (exec *execCtx) execute() {
	exec.log.Debug("serving request...")

	// perform local operation
	exec.executeLocal()

	exec.analyzeStatus(true)
}

func (exec *execCtx) analyzeStatus(execCnr bool) {
	// analyze local result
	switch exec.status {
	default:
		exec.log.Debug("operation finished with error",
			logger.FieldString("error", exec.err.Error()),
		)
	case statusOK:
		exec.log.Debug("operation finished successfully")
	}

	if execCnr {
		exec.executeOnContainer()
		exec.analyzeStatus(false)
	}
}
