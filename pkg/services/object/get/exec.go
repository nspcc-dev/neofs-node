package getsvc

import (
	"context"
	"crypto/ecdsa"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
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

	head bool

	curProcEpoch uint64
}

type execOption func(*execCtx)

const (
	statusUndefined int = iota
	statusOK
	statusINHUMED
	statusVIRTUAL
	statusOutOfRange
)

func headOnly() execOption {
	return func(c *execCtx) {
		c.head = true
	}
}

func withPayloadRange(r *objectSDK.Range) execOption {
	return func(c *execCtx) {
		c.prm.rng = r
	}
}

func (exec *execCtx) setLogger(l *logger.Logger) {
	req := "GET"
	if exec.headOnly() {
		req = "HEAD"
	} else if exec.ctxRange() != nil {
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

func (exec execCtx) isChild(obj *object.Object) bool {
	par := obj.GetParent()
	return par != nil && equalAddresses(exec.address(), par.Address())
}

func (exec execCtx) key() *ecdsa.PrivateKey {
	return exec.prm.common.PrivateKey()
}

func (exec execCtx) callOptions() []client.CallOption {
	return exec.prm.common.RemoteCallOptions(
		util.WithNetmapEpoch(exec.curProcEpoch),
		util.WithKey(exec.key()))
}

func (exec execCtx) remotePrm() *client.GetObjectParams {
	return new(client.GetObjectParams).
		WithAddress(exec.prm.Address()).
		WithRawFlag(exec.prm.RawFlag())
}

func (exec *execCtx) canAssemble() bool {
	return exec.svc.assembly && !exec.isRaw() && !exec.headOnly()
}

func (exec *execCtx) splitInfo() *objectSDK.SplitInfo {
	return exec.infoSplit
}

func (exec *execCtx) containerID() *cid.ID {
	return exec.address().ContainerID()
}

func (exec *execCtx) ctxRange() *objectSDK.Range {
	return exec.prm.rng
}

func (exec *execCtx) headOnly() bool {
	return exec.head
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

func (exec *execCtx) generateTraverser(addr *objectSDK.Address) (*placement.Traverser, bool) {
	t, err := exec.svc.traverserGenerator.GenerateTraverser(addr, exec.curProcEpoch)

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

func (exec *execCtx) getChild(id *objectSDK.ID, rng *objectSDK.Range, withHdr bool) (*object.Object, bool) {
	w := NewSimpleObjectWriter()

	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.objWriter = w
	p.SetRange(rng)

	addr := objectSDK.NewAddress()
	addr.SetContainerID(exec.address().ContainerID())
	addr.SetObjectID(id)

	p.WithAddress(addr)

	exec.statusError = exec.svc.get(exec.context(), p.commonPrm, withPayloadRange(rng))

	child := w.Object()
	ok := exec.status == statusOK

	if ok && withHdr && !exec.isChild(child) {
		exec.status = statusUndefined
		exec.err = errors.New("wrong child header")

		exec.log.Debug("parent address in child object differs")
	}

	return child, ok
}

func (exec *execCtx) headChild(id *objectSDK.ID) (*object.Object, bool) {
	childAddr := objectSDK.NewAddress()
	childAddr.SetContainerID(exec.containerID())
	childAddr.SetObjectID(id)

	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.WithAddress(childAddr)

	prm := HeadPrm{
		commonPrm: p.commonPrm,
	}

	w := NewSimpleObjectWriter()
	prm.SetHeaderWriter(w)

	err := exec.svc.Head(exec.context(), prm)

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
		child := w.Object()

		if child.ParentID() != nil && !exec.isChild(child) {
			exec.status = statusUndefined

			exec.log.Debug("parent address in child object differs")
		} else {
			exec.status = statusOK
			exec.err = nil
		}

		return child, true
	}
}

func (exec execCtx) remoteClient(info clientcore.NodeInfo) (getClient, bool) {
	c, err := exec.svc.clientCache.get(info)

	switch {
	default:
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not construct remote node client")
	case err == nil:
		return c, true
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
	if exec.headOnly() {
		return true
	}

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

// isForwardingEnabled returns true if common execution
// parameters has request forwarding closure set.
func (exec execCtx) isForwardingEnabled() bool {
	return exec.prm.forwarder != nil
}

// disableForwarding removes request forwarding closure from common
// parameters, so it won't be inherited in new execution contexts.
func (exec *execCtx) disableForwarding() {
	exec.prm.SetRequestForwarder(nil)
}
