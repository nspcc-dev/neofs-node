package putsvc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	svcutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protowire"
)

type preparedObjectTarget interface {
	WriteObject(context.Context, *objectSDK.Object, object.ContentMeta) error
	Close() (oid.ID, error)
}

type binReplicationContext struct {
	context.Context
	hdr    []byte
	pldFld []byte
}

type distributedTarget struct {
	ctx context.Context

	traversal traversal

	remotePool, localPool util.WorkerPool

	obj     *objectSDK.Object
	objMeta object.ContentMeta

	pldFld []byte
	pldOff int // after field tag prefix

	nodeTargetInitializer func(nodeDesc) preparedObjectTarget

	isLocalKey func([]byte) bool

	relay func(nodeDesc) error

	fmt *object.FormatValidator

	log *zap.Logger
}

// parameters and state of container traversal.
type traversal struct {
	placementBuilder placement.Builder

	opts []placement.Option

	// need of additional broadcast after the object is saved
	extraBroadcastEnabled bool

	// mtx protects mExclude map.
	mtx sync.RWMutex

	// container nodes which was processed during the primary object placement
	mExclude map[string]struct{}
}

// updates traversal parameters after the primary placement finish and
// returns true if additional container broadcast is needed.
func (x *traversal) submitPrimaryPlacementFinish() bool {
	if x.extraBroadcastEnabled {
		// do not track success during container broadcast (best-effort)
		x.opts = append(x.opts, placement.WithoutSuccessTracking())

		// avoid 2nd broadcast
		x.extraBroadcastEnabled = false

		return true
	}

	return false
}

// marks the container node as processed during the primary object placement.
func (x *traversal) submitProcessed(n placement.Node) {
	if x.extraBroadcastEnabled {
		key := string(n.PublicKey())

		x.mtx.Lock()
		if x.mExclude == nil {
			x.mExclude = make(map[string]struct{}, 1)
		}

		x.mExclude[key] = struct{}{}
		x.mtx.Unlock()
	}
}

// checks if specified node was processed during the primary object placement.
func (x *traversal) processed(n placement.Node) bool {
	x.mtx.RLock()
	_, ok := x.mExclude[string(n.PublicKey())]
	x.mtx.RUnlock()
	return ok
}

type nodeDesc struct {
	local bool

	info placement.Node
}

// errIncompletePut is returned if processing on a container fails.
type errIncompletePut struct {
	singleErr error // error from the last responding node
}

func (x errIncompletePut) Error() string {
	const commonMsg = "incomplete object PUT by placement"

	if x.singleErr != nil {
		return fmt.Sprintf("%s: %v", commonMsg, x.singleErr)
	}

	return commonMsg
}

func (t *distributedTarget) WriteHeader(obj *objectSDK.Object) error {
	t.obj = obj

	return nil
}

func (t *distributedTarget) Write(p []byte) (n int, err error) {
	if t.pldOff == 0 {
		sz := t.obj.PayloadSize()
		if sz > 0 {
			t.pldFld = protowire.AppendTag(t.pldFld, 4, protowire.BytesType)
			t.pldFld = protowire.AppendVarint(t.pldFld, sz)
			t.pldOff = len(t.pldFld)
		}
	}
	t.pldFld = append(t.pldFld, p...)

	return len(p), nil
}

func (t *distributedTarget) Close() (oid.ID, error) {
	defer func() {
		// TODO: keep using the buffer during slicing
		putPayload(t.pldFld)
		t.pldFld = nil
		t.pldOff = 0
	}()

	t.obj.SetPayload(t.pldFld[t.pldOff:])

	var err error

	if t.objMeta, err = t.fmt.ValidateContent(t.obj); err != nil {
		return oid.ID{}, fmt.Errorf("(%T) could not validate payload content: %w", t, err)
	}

	if len(t.obj.Children()) > 0 {
		// enabling extra broadcast for linking objects
		t.traversal.extraBroadcastEnabled = true
	}

	return t.iteratePlacement(t.ctx, t.sendObject)
}

func (t *distributedTarget) sendObject(ctx context.Context, node nodeDesc) error {
	if !node.local && t.relay != nil {
		return t.relay(node)
	}

	target := t.nodeTargetInitializer(node)

	if err := target.WriteObject(ctx, t.obj, t.objMeta); err != nil {
		return fmt.Errorf("could not write header: %w", err)
	} else if _, err := target.Close(); err != nil {
		return fmt.Errorf("could not close object stream: %w", err)
	}
	return nil
}

func (t *distributedTarget) iteratePlacement(ctx context.Context, f func(context.Context, nodeDesc) error) (oid.ID, error) {
	id, _ := t.obj.ID()

	placementBuilder := t.traversal.placementBuilder
	var placementIntrcpt *placementInterceptor

	if _, ok := ctx.(*binReplicationContext); !ok {
		placementIntrcpt = &placementInterceptor{
			base:       t.traversal.placementBuilder,
			isLocalKey: t.isLocalKey,
		}
		placementBuilder = placementIntrcpt
	}

	traverser, err := placement.NewTraverser(
		append(t.traversal.opts, placement.UseBuilder(placementBuilder), placement.ForObject(id))...,
	)
	if err != nil {
		return oid.ID{}, fmt.Errorf("(%T) could not create object placement traverser: %w", t, err)
	}

	if placementIntrcpt != nil && placementIntrcpt.withLocalAndRemoteNodes {
		hdr := noPayloadProtobuf(t.obj)
		ctx = &binReplicationContext{
			Context: ctx,
			hdr:     hdr,
			pldFld:  t.pldFld,
		}
	}

	var resErr atomic.Value

loop:
	for {
		addrs := traverser.Next()
		if len(addrs) == 0 {
			break
		}

		wg := new(sync.WaitGroup)

		for i := range addrs {
			if t.traversal.processed(addrs[i]) {
				// it can happen only during additional container broadcast
				continue
			}

			wg.Add(1)

			addr := addrs[i]

			isLocal := t.isLocalKey(addr.PublicKey())

			var workerPool util.WorkerPool

			if isLocal {
				workerPool = t.localPool
			} else {
				workerPool = t.remotePool
			}

			if err := workerPool.Submit(func() {
				defer wg.Done()

				err := f(ctx, nodeDesc{local: isLocal, info: addr})

				// mark the container node as processed in order to exclude it
				// in subsequent container broadcast. Note that we don't
				// process this node during broadcast if primary placement
				// on it failed.
				t.traversal.submitProcessed(addr)

				if err != nil {
					resErr.Store(err)
					svcutil.LogServiceError(t.log, "PUT", addr.Addresses(), err)
					return
				}

				traverser.SubmitSuccess()
			}); err != nil {
				wg.Done()

				svcutil.LogWorkerPoolError(t.log, "PUT", err)

				break loop
			}
		}

		wg.Wait()
	}

	if !traverser.Success() {
		var err errIncompletePut

		err.singleErr, _ = resErr.Load().(error)

		return oid.ID{}, err
	}

	// perform additional container broadcast if needed
	if t.traversal.submitPrimaryPlacementFinish() {
		_, err = t.iteratePlacement(ctx, f)
		if err != nil {
			t.log.Error("additional container broadcast failure",
				zap.Error(err),
			)

			// we don't fail primary operation because of broadcast failure
		}
	}

	id, _ = t.obj.ID()

	return id, nil
}

func noPayloadProtobuf(obj *objectSDK.Object) []byte {
	objV2 := obj.CutPayload().ToV2()
	id := objV2.GetObjectID()
	sig := objV2.GetSignature()
	hdr := objV2.GetHeader()

	const (
		fieldNumID        = 1
		fieldNumSignature = 2
		fieldNumHeader    = 3
		fieldNumPayload   = 4
	)

	idSize := id.StableSize()
	sigSize := sig.StableSize()
	hdrSize := hdr.StableSize()

	s := protowire.SizeTag(fieldNumID) + protowire.SizeBytes(idSize) +
		protowire.SizeTag(fieldNumSignature) + protowire.SizeBytes(sigSize) +
		protowire.SizeTag(fieldNumHeader) + protowire.SizeBytes(hdrSize)
	// protowire.SizeTag(fieldNumPayload) + protowire.SizeVarint(uint64(ps))

	// TODO: separate buffer
	b := make([]byte, 0, s)

	b = protowire.AppendTag(b, fieldNumID, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(idSize))
	b = b[:len(b)+idSize]
	id.StableMarshal(b[len(b)-idSize:])
	b = protowire.AppendTag(b, fieldNumSignature, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(sigSize))
	b = b[:len(b)+sigSize]
	sig.StableMarshal(b[len(b)-sigSize:])
	b = protowire.AppendTag(b, fieldNumHeader, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(hdrSize))
	b = b[:len(b)+hdrSize]
	hdr.StableMarshal(b[len(b)-hdrSize:])
	// pldOff := len(b)
	// b = protowire.AppendTag(b, fieldNumPayload, protowire.BytesType)
	// b = protowire.AppendVarint(b, uint64(ps))

	return b
}

type placementInterceptor struct {
	base placement.Builder

	isLocalKey func([]byte) bool

	withLocalAndRemoteNodes bool
}

func (x *placementInterceptor) BuildPlacement(cnr cid.ID, obj *oid.ID, storagepolicy netmap.PlacementPolicy) ([][]netmap.NodeInfo, error) {
	cnrNodesSets, err := x.base.BuildPlacement(cnr, obj, storagepolicy)
	if err != nil {
		return nil, err
	}

	var foundLocal, foundRemote bool
	for i := range cnrNodesSets {
		for j := range cnrNodesSets[i] {
			pubKey := cnrNodesSets[i][j].PublicKey()
			isLocal := x.isLocalKey(pubKey)

			if !foundLocal {
				foundLocal = isLocal
			}

			if !foundRemote {
				foundRemote = !isLocal
			}

			if foundLocal && foundRemote {
				x.withLocalAndRemoteNodes = true
				return cnrNodesSets, nil
			}
		}
	}

	return cnrNodesSets, nil
}
