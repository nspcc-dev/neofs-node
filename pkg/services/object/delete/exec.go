package deletesvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
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

	ctx context.Context

	prm Prm

	statusError

	log *zap.Logger

	tombstoneObj *object.Object
}

const (
	statusUndefined int = iota
	statusOK
)

func (exec *execCtx) setLogger(l *zap.Logger) {
	exec.log = l.With(
		zap.String("request", "DELETE"),
		zap.Stringer("address", exec.address()),
		zap.Bool("local", exec.isLocal()),
		zap.Bool("with session", exec.prm.common.SessionToken() != nil),
		zap.Bool("with bearer", exec.prm.common.BearerToken() != nil),
	)
}

func (exec execCtx) context() context.Context {
	return exec.ctx
}

func (exec execCtx) isLocal() bool {
	return exec.prm.common.LocalOnly()
}

func (exec *execCtx) address() oid.Address {
	return exec.prm.addr
}

func (exec *execCtx) containerID() cid.ID {
	return exec.prm.addr.Container()
}

func (exec *execCtx) commonParameters() *util.CommonPrm {
	return exec.prm.common
}

func (exec *execCtx) newAddress(id oid.ID) oid.Address {
	var a oid.Address
	a.SetObject(id)
	a.SetContainer(exec.containerID())

	return a
}

func (exec *execCtx) saveTombstone() bool {
	id, err := exec.svc.placer.put(exec)

	if err == nil {
		exec.status = statusOK
		exec.err = nil

		exec.prm.tombAddrWriter.
			SetAddress(exec.newAddress(*id))

		return true
	}

	exec.status = statusUndefined
	exec.err = err

	exec.log.Debug("could not save the tombstone",
		zap.Error(err),
	)
	return false
}
