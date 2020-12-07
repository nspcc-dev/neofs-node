package getsvc

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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

	prm RangePrm

	statusError

	infoSplit *objectSDK.SplitInfo

	log *logger.Logger

	collectedObject *object.Object

	curOff uint64
}

const (
	statusUndefined int = iota
	statusOK
	statusINHUMED
	statusVIRTUAL
	statusOutOfRange
)

func (exec *execCtx) setLogger(l *logger.Logger) {
	req := "GET"
	if exec.ctxRange() != nil {
		req = "GET_RANGE"
	}

	exec.log = l.With(
		zap.String("request", req),
		zap.Stringer("address", exec.address()),
		zap.Bool("raw", exec.isRaw()),
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

func (exec execCtx) isRaw() bool {
	return exec.prm.RawFlag()
}

func (exec execCtx) address() *objectSDK.Address {
	return exec.prm.Address()
}

func (exec execCtx) key() *ecdsa.PrivateKey {
	return exec.prm.key
}

func (exec execCtx) callOptions() []client.CallOption {
	return exec.prm.callOpts
}

func (exec execCtx) remotePrm() *client.GetObjectParams {
	return new(client.GetObjectParams).
		WithAddress(exec.prm.Address()).
		WithRawFlag(exec.prm.RawFlag())
}

func (exec *execCtx) canAssemble() bool {
	return exec.svc.assembly && !exec.isRaw()
}

func (exec *execCtx) splitInfo() *objectSDK.SplitInfo {
	return exec.infoSplit
}

func (exec *execCtx) containerID() *container.ID {
	return exec.address().ContainerID()
}

func (exec *execCtx) ctxRange() *objectSDK.Range {
	return exec.prm.rng
}

func (exec *execCtx) generateTraverser(addr *objectSDK.Address) (*placement.Traverser, bool) {
	t, err := exec.svc.traverserGenerator.GenerateTraverser(addr)

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

func (exec *execCtx) getChild(id *objectSDK.ID, rng *objectSDK.Range) (*object.Object, bool) {
	w := newSimpleObjectWriter()

	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.objWriter = w
	p.SetRange(rng)

	addr := objectSDK.NewAddress()
	addr.SetContainerID(exec.address().ContainerID())
	addr.SetObjectID(id)

	p.WithAddress(addr)

	exec.statusError = exec.svc.get(exec.context(), p)

	return w.object(), exec.status == statusOK
}

func (exec *execCtx) headChild(id *objectSDK.ID) (*object.Object, bool) {
	childAddr := objectSDK.NewAddress()
	childAddr.SetContainerID(exec.containerID())
	childAddr.SetObjectID(id)

	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.WithAddress(childAddr)

	header, err := exec.svc.headSvc.head(exec.context(), Prm{
		commonPrm: p.commonPrm,
	})

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not get child object header",
			zap.Stringer("child ID", id),
			zap.String("error", err.Error()),
		)

		return nil, false
	case err == nil:
		exec.status = statusOK
		exec.err = nil

		return header, true
	}
}

func (exec execCtx) remoteClient(node *network.Address) (getClient, bool) {
	ipAddr, err := node.IPAddrString()

	log := exec.log.With(zap.Stringer("node", node))

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		log.Debug("could not calculate node IP address")
	case err == nil:
		c, err := exec.svc.clientCache.get(exec.key(), ipAddr)

		switch {
		default:
			exec.status = statusUndefined
			exec.err = err

			log.Debug("could not construct remote node client")
		case err == nil:
			return c, true
		}
	}

	return nil, false
}

func mergeSplitInfo(dst, src *objectSDK.SplitInfo) {
	if last := src.LastPart(); last != nil {
		dst.SetLastPart(last)
	}

	if link := src.Link(); link != nil {
		dst.SetLink(link)
	}

	if splitID := src.SplitID(); splitID != nil {
		dst.SetSplitID(splitID)
	}
}

func (exec *execCtx) writeCollectedHeader() bool {
	if exec.ctxRange() != nil {
		return true
	}

	err := exec.prm.objWriter.WriteHeader(
		object.NewRawFromObject(exec.collectedObject).CutPayload().Object(),
	)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write header",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}

	return exec.status == statusOK
}

func (exec *execCtx) writeObjectPayload(obj *object.Object) bool {
	err := exec.prm.objWriter.WriteChunk(obj.Payload())

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write payload chunk",
			zap.String("error", err.Error()),
		)
	case err == nil:
		exec.status = statusOK
		exec.err = nil
	}

	return err == nil
}

func (exec *execCtx) writeCollectedObject() {
	if ok := exec.writeCollectedHeader(); ok {
		exec.writeObjectPayload(exec.collectedObject)
	}
}
