package getsvc

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

const (
	// streamChunkSize is the size of the chunk that is used to read/write
	// object payload from/to the stream.
	streamChunkSize = 256 * 1024 // 256 KiB
)

var errStreamFailure = errors.New("stream failure")

type statusError struct {
	status int
	err    error
}

type execCtx struct {
	svc *Service

	ctx context.Context

	prm          RangePrm
	prmRangeHash *RangeHashPrm

	statusError

	infoSplit *object.SplitInfo

	// If debug level is enabled, all messages also include info about processing request.
	log *zap.Logger

	collectedHeader *object.Object
	collectedReader io.ReadCloser

	curOff uint64

	head bool

	// range assembly (V1 split) helpers
	lastChildID    oid.ID
	lastChildRange object.Range

	nodeLists [][]netmap.NodeInfo
	repRules  []uint
}

type execOption func(*execCtx)

const (
	statusUndefined int = iota
	statusOK
	statusVIRTUAL
	statusAPIResponse
	statusNotFound
)

func headOnly() execOption {
	return func(c *execCtx) {
		c.head = true
	}
}

func withPayloadRange(r *object.Range) execOption {
	return func(c *execCtx) {
		c.prm.rng = r
	}
}

func withHash(p *RangeHashPrm) execOption {
	return func(ctx *execCtx) {
		ctx.prmRangeHash = p
	}
}

func withLogger(l *zap.Logger) execOption {
	return func(ctx *execCtx) {
		ctx.log = l
	}
}

func withPreSortedContainerNodes(nodeLists [][]netmap.NodeInfo, repRules []uint) execOption {
	return func(ctx *execCtx) {
		ctx.nodeLists = nodeLists
		ctx.repRules = repRules
	}
}

func (exec *execCtx) setLogger(l *zap.Logger) {
	if l.Level() != zap.DebugLevel {
		exec.log = l
		return
	}

	reqFields := []zap.Field{
		zap.Stringer("address", exec.address()),
		zap.Bool("raw", exec.isRaw()),
		zap.Bool("local", exec.isLocal()),
		zap.Bool("with session", exec.prm.common.SessionToken() != nil),
		zap.Bool("with bearer", exec.prm.common.BearerToken() != nil),
	}

	switch {
	case exec.headOnly():
		reqFields = append(reqFields, zap.String("request", "HEAD"))
	case exec.ctxRange() != nil:
		reqFields = append(reqFields,
			zap.String("request", "GET_RANGE"),
			zap.String("original range", prettyRange(exec.ctxRange())))
	default:
		reqFields = append(reqFields, zap.String("request", "GET"))
	}

	exec.log = l.With(reqFields...)
}

func (exec execCtx) context() context.Context {
	return exec.ctx
}

func (exec execCtx) isLocal() bool {
	return exec.prm.common.LocalOnly()
}

func (exec execCtx) isRaw() bool {
	return exec.prm.raw
}

func (exec execCtx) address() oid.Address {
	return exec.prm.addr
}

// isChild checks if reading object is a parent of the given object.
// Object without reference to the parent (only children with the parent header
// have it) is automatically considered as child: this should be guaranteed by
// upper level logic.
func (exec execCtx) isChild(obj *object.Object) bool {
	par := obj.Parent()
	return par == nil || equalAddresses(exec.address(), par.Address())
}

func (exec execCtx) key() (*ecdsa.PrivateKey, error) {
	if exec.prm.signerKey != nil {
		// the key has already been requested and
		// cached in the previous operations
		return exec.prm.signerKey, nil
	}

	key, err := exec.svc.keyStore.GetKey(nil)
	if err != nil {
		return nil, err
	}
	if tokV2 := exec.prm.common.SessionTokenV2(); tokV2 != nil {
		// For V2 tokens, the key is stored as the subjects
		if keyForSession, err := exec.svc.keyStore.GetKeyBySubjects(tokV2.Issuer(), tokV2.Subjects()); err == nil {
			key = keyForSession
		} else if exec.svc.nnsResolver != nil {
			nodeUser := user.NewFromECDSAPublicKey(key.PublicKey)
			ok, authErr := tokV2.AssertAuthority(nodeUser, exec.svc.nnsResolver)
			if authErr != nil {
				return nil, fmt.Errorf("assert authority for session v2 token: %w", authErr)
			}
			if !ok {
				return nil, fmt.Errorf("session v2 token authority assertion failed")
			}
			// node key is already in key
		} else {
			return nil, fmt.Errorf("get key for session v2 token: %w", err)
		}
	} else if tok := exec.prm.common.SessionToken(); tok != nil {
		key, err = exec.svc.keyStore.GetKey(&util.SessionInfo{
			ID:    tok.ID(),
			Owner: tok.Issuer(),
		})
		if err != nil {
			return nil, err
		}
	}

	return key, nil
}

func (exec *execCtx) canAssemble() bool {
	return !exec.isRaw() && !exec.headOnly()
}

func (exec *execCtx) splitInfo() *object.SplitInfo {
	return exec.infoSplit
}

func (exec *execCtx) containerID() cid.ID {
	return exec.address().Container()
}

func (exec *execCtx) ctxRange() *object.Range {
	return exec.prm.rng
}

func (exec *execCtx) headOnly() bool {
	return exec.head
}

// copyChild fetches child object payload and streams it directly into current exec writer.
// Returns true if full payload (or requested range) was successfully written and, if requested, header validated.
func (exec *execCtx) copyChild(id oid.ID, rng *object.Range, withHdr bool) bool {
	log := exec.log
	if rng != nil {
		log = log.With(zap.String("child range", prettyRange(rng)))
	}

	childWriter := newDirectChildWriter(exec.prm.objWriter)

	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.objWriter = childWriter
	p.SetRange(rng)
	p.addr.SetContainer(exec.containerID())
	p.addr.SetObject(id)

	exec.statusError = exec.svc.get(exec.context(), p.commonPrm, withPayloadRange(rng), withLogger(log))

	hdr := childWriter.hdr
	ok := exec.status == statusOK

	if ok && withHdr {
		if hdr == nil {
			return false
		}
		if !exec.isChild(hdr) {
			exec.status = statusUndefined
			exec.err = errors.New("wrong child header")
			exec.log.Debug("parent address in child object differs")
		}
	}

	return ok
}

func (exec *execCtx) headChild(id oid.ID) (*object.Object, bool) {
	p := exec.prm
	p.common = p.common.WithLocalOnly(false)
	p.addr.SetContainer(exec.containerID())
	p.addr.SetObject(id)

	prm := HeadPrm{
		commonPrm: p.commonPrm,
	}

	w := NewSimpleObjectWriter()
	prm.SetHeaderWriter(w)

	err := exec.svc.Head(exec.context(), prm)

	if err == nil {
		child := w.Object()

		if !exec.isChild(child) {
			exec.status = statusUndefined

			exec.log.Debug("parent address in child object differs")
		} else {
			exec.status = statusOK
			exec.err = nil
		}

		return child, true
	}
	exec.status = statusUndefined
	exec.err = err

	exec.log.Debug("could not get child object header",
		zap.Stringer("child ID", id),
		zap.Error(err),
	)

	return nil, false
}

func (exec execCtx) remoteClient(info clientcore.NodeInfo) (getClient, bool) {
	c, err := exec.svc.clientCache.get(info)

	if err == nil {
		return c, true
	}
	exec.status = statusUndefined
	exec.err = err
	exec.log.Debug("could not construct remote node client")
	return nil, false
}

func mergeSplitInfo(dst, src *object.SplitInfo) {
	if last := src.GetLastPart(); !last.IsZero() {
		dst.SetLastPart(last)
	}

	if link := src.GetLink(); !link.IsZero() {
		dst.SetLink(link)
	}

	if first := src.GetFirstPart(); !first.IsZero() {
		dst.SetFirstPart(first)
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
		exec.collectedHeader.CutPayload(),
	)

	if err == nil {
		exec.status = statusOK
		exec.err = nil
	} else {
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write header",
			zap.Error(err),
		)
	}

	return exec.status == statusOK
}

func (exec *execCtx) writeObjectPayload(obj *object.Object, reader io.ReadCloser) bool {
	if exec.headOnly() {
		return true
	}

	var err error
	if reader != nil {
		defer func() {
			err := reader.Close()
			if err != nil {
				exec.log.Debug("error while closing payload reader", zap.Error(err))
			}
		}()
		err = copyPayloadStream(exec.prm.objWriter, reader)
	} else {
		err = exec.prm.objWriter.WriteChunk(obj.Payload())
	}

	if err == nil {
		exec.status = statusOK
		exec.err = nil
	} else {
		exec.status = statusUndefined
		exec.err = err

		exec.log.Debug("could not write payload chunk",
			zap.Error(err),
		)
	}

	return err == nil
}

func copyObject(w ObjectWriter, obj object.Object) error {
	if err := w.WriteHeader(&obj); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if err := w.WriteChunk(obj.Payload()); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	return nil
}

func copyObjectStream(w ObjectWriter, h object.Object, r io.Reader) error {
	if err := w.WriteHeader(&h); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	return copyPayloadStream(w, r)
}

// copyPayloadStream writes payload from stream to writer.
func copyPayloadStream(w ChunkWriter, r io.Reader) error {
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	_, err := copyPayloadStreamBuffer(w, r, *buf.(*[]byte))
	return err
}

// returns number of written bytes. Returns errResponseStreamFailure on w failure.
func copyPayloadStreamBuffer(w ChunkWriter, r io.Reader, buf []byte) (uint64, error) {
	for done := uint64(0); ; {
		n, err := r.Read(buf)
		if n > 0 {
			if writeErr := w.WriteChunk(buf[:n]); writeErr != nil {
				return done, fmt.Errorf("%w: %w", errStreamFailure, writeErr)
			}
			done += uint64(n)
		}

		if errors.Is(err, io.EOF) {
			return done, nil
		}
		if err != nil {
			return done, fmt.Errorf("read next payload chunk: %w", err)
		}
	}
}

func (exec *execCtx) writeCollectedObject() {
	if ok := exec.writeCollectedHeader(); ok {
		exec.writeObjectPayload(exec.collectedHeader, exec.collectedReader)
	}
}

// isForwardingEnabled returns true if common execution
// parameters has request forwarding closure set.
func (exec execCtx) isForwardingEnabled() bool {
	return exec.prm.forwarder != nil
}

// isRangeHashForwardingEnabled returns true if common execution
// parameters has GETRANGEHASH request forwarding closure set.
func (exec execCtx) isRangeHashForwardingEnabled() bool {
	return exec.prm.rangeForwarder != nil
}

// disableForwarding removes request forwarding closure from common
// parameters, so it won't be inherited in new execution contexts.
func (exec *execCtx) disableForwarding() {
	exec.prm.SetRequestForwarder(nil)
	exec.prm.SetRangeHashRequestForwarder(nil)
}
