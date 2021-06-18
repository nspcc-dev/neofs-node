package searchsvc

import (
	"context"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
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

	log *logger.Logger

	curProcEpoch uint64
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

func (exec *execCtx) setLogger(l *logger.Logger) {
	exec.log = l.With(
		zap.String("request", "SEARCH"),
		zap.Stringer("container", exec.containerID()),
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

func (exec *execCtx) containerID() *cid.ID {
	return exec.prm.ContainerID()
}

func (exec *execCtx) searchFilters() objectSDK.SearchFilters {
	return exec.prm.SearchFilters()
}

func (exec *execCtx) netmapEpoch() uint64 {
	return exec.prm.common.NetmapEpoch()
}

func (exec *execCtx) netmapLookupDepth() uint64 {
	return exec.prm.common.NetmapLookupDepth()
}

func (exec *execCtx) initEpoch() bool {
	exec.curProcEpoch = exec.netmapEpoch()
	if exec.curProcEpoch > 0 {
		return true
	}

	e, err := exec.svc.currentEpochReceiver.currentEpoch()

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not get current epoch number",
			zap.String("error", err.Error()),
		)

		return false
	case err == nil:
		exec.curProcEpoch = e
		return true
	}
}

func (exec *execCtx) generateTraverser(cid *cid.ID) (*placement.Traverser, bool) {
	t, err := exec.svc.traverserGenerator.generateTraverser(cid, exec.curProcEpoch)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not generate container traverser",
			zap.String("error", err.Error()),
		)

		return nil, false
	case err == nil:
		return t, true
	}
}

func (exec execCtx) remoteClient(node network.Address) (searchClient, bool) {
	log := exec.log.With(zap.Stringer("node", node))

	c, err := exec.svc.clientConstructor.get(node)
	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		log.Debug("could not construct remote node client")
	case err == nil:
		return c, true
	}

	return nil, false
}

func (exec *execCtx) writeIDList(ids []*objectSDK.ID) {
	err := exec.prm.writer.WriteIDs(ids)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write object identifiers",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}
}
