package deletesvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"go.uber.org/zap"
)

// Delete serves requests to remote the objects.
func (s *Service) Delete(ctx context.Context, prm Prm) error {
	// If session token is not found we will fail during tombstone PUT.
	// Here we fail immediately to ensure no unnecessary network communication is done.
	if tok := prm.common.SessionToken(); tok != nil {
		_, err := s.keyStorage.GetKey(&util.SessionInfo{
			ID:    tok.ID(),
			Owner: tok.Issuer(),
		})
		if err != nil {
			return err
		}
	}

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
			zap.Error(exec.err),
		)

		if execCnr {
			exec.executeOnContainer()
			exec.analyzeStatus(false)
		}
	}
}
