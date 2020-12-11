package deletesvc

import (
	"context"

	"go.uber.org/zap"
)

// Delete serves requests to remote the objects.
func (s *Service) Delete(ctx context.Context, prm Prm) error {
	exec := &execCtx{
		svc: s,
		ctx: ctx,
		prm: prm,
	}

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
	case statusOK:
		exec.log.Debug("operation finished successfully")
	default:
		exec.log.Debug("operation finished with error",
			zap.String("error", exec.err.Error()),
		)

		if execCnr {
			exec.executeOnContainer()
			exec.analyzeStatus(false)
		}
	}
}
