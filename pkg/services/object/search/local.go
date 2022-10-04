package searchsvc

import "github.com/nspcc-dev/neofs-node/pkg/util/logger"

func (exec *execCtx) executeLocal() {
	ids, err := exec.svc.localStorage.search(exec)

	if err != nil {
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("local operation failed",
			logger.FieldError(err),
		)

		return
	}

	exec.writeIDList(ids)
}
