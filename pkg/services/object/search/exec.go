package searchsvc

import (
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

type statusError struct {
	status int
	err    error
}

type execCtx struct {
	svc *Service

	prm Prm

	statusError

	log *zap.Logger
}

const (
	statusUndefined int = iota
	statusOK
)

func (exec *execCtx) prepare() {
	if _, ok := exec.prm.writer.(*uniqueIDWriter); !ok {
		exec.prm.writer = newUniqueAddressWriter(exec.prm.writer)
	}
}

func (exec *execCtx) setLogger(l *zap.Logger) {
	exec.log = l.With(
		zap.String("request", "SEARCH"),
		zap.Stringer("container", exec.containerID()),
		zap.Bool("local", exec.isLocal()),
		zap.Bool("with session", exec.prm.common.SessionToken() != nil),
		zap.Bool("with bearer", exec.prm.common.BearerToken() != nil),
	)
}

func (exec execCtx) isLocal() bool {
	return exec.prm.common.LocalOnly()
}

func (exec *execCtx) containerID() cid.ID {
	return exec.prm.cnr
}

func (exec *execCtx) searchFilters() object.SearchFilters {
	return exec.prm.filters
}

func (exec *execCtx) writeIDList(ids []oid.ID) {
	var err error

	if len(ids) > 0 {
		err = exec.prm.writer.WriteIDs(ids)
	}

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write object identifiers",
			zap.Error(err),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}
}
