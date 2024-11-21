package searchsvc

import (
	"go.uber.org/zap"
)

func (exec *execCtx) executeLocal() {
	ids, err := exec.svc.localStorage.search(exec)

	if err != nil {
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("local operation failed",
			zap.Error(err),
		)

		return
	}

	exec.writeIDList(ids)
}
