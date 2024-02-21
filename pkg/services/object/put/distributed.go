package putsvc

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	svcutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

type preparedObjectTarget interface {
	WriteObject(*objectSDK.Object, object.ContentMeta, encodedObject) error
	Close() (oid.ID, error)
}

type distributedTarget struct {
	traversal traversal

	remotePool, localPool util.WorkerPool

	obj     *objectSDK.Object
	objMeta object.ContentMeta

	nodeTargetInitializer func(nodeDesc) preparedObjectTarget

	isLocalKey func([]byte) bool

	relay func(nodeDesc) error

	fmt *object.FormatValidator

	log *zap.Logger

	localOnly            bool
	localNodeInContainer bool
	localNodeSigner      neofscrypto.Signer
	// - object if localOnly
	// - replicate request if localNodeInContainer
	// - payload otherwise
	encodedObject encodedObject
}

// parameters and state of container traversal.
type traversal struct {
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

func (t *distributedTarget) WriteHeader(hdr *objectSDK.Object) error {
	payloadLen := hdr.PayloadSize()
	if payloadLen > math.MaxInt {
		return fmt.Errorf("too big payload of physically stored for this server %d > %d", payloadLen, math.MaxInt)
	}

	if t.localNodeInContainer {
		var err error
		if t.localOnly {
			t.encodedObject, err = encodeObjectWithoutPayload(*hdr, int(payloadLen))
		} else {
			t.encodedObject, err = encodeReplicateRequestWithoutPayload(t.localNodeSigner, *hdr, int(payloadLen))
		}
		if err != nil {
			return err
		}
	} else if payloadLen > 0 {
		t.encodedObject = encodedObject{b: getBuffer(int(payloadLen))}
	}

	t.obj = hdr

	return nil
}

func (t *distributedTarget) Write(p []byte) (n int, err error) {
	t.encodedObject.b = append(t.encodedObject.b, p...)

	return len(p), nil
}

func (t *distributedTarget) Close() (oid.ID, error) {
	defer putBuffer(t.encodedObject.b)

	t.obj.SetPayload(t.encodedObject.b[t.encodedObject.pldOff:])

	var err error

	if t.objMeta, err = t.fmt.ValidateContent(t.obj); err != nil {
		return oid.ID{}, fmt.Errorf("(%T) could not validate payload content: %w", t, err)
	}

	if len(t.obj.Children()) > 0 {
		// enabling extra broadcast for linking objects
		t.traversal.extraBroadcastEnabled = true
	}

	return t.iteratePlacement(t.sendObject)
}

func (t *distributedTarget) sendObject(node nodeDesc) error {
	if !node.local && t.relay != nil {
		return t.relay(node)
	}

	target := t.nodeTargetInitializer(node)

	if err := target.WriteObject(t.obj, t.objMeta, t.encodedObject); err != nil {
		return fmt.Errorf("could not write header: %w", err)
	} else if _, err := target.Close(); err != nil {
		return fmt.Errorf("could not close object stream: %w", err)
	}
	return nil
}

func (t *distributedTarget) iteratePlacement(f func(nodeDesc) error) (oid.ID, error) {
	id, _ := t.obj.ID()

	traverser, err := placement.NewTraverser(
		append(t.traversal.opts, placement.ForObject(id))...,
	)
	if err != nil {
		return oid.ID{}, fmt.Errorf("(%T) could not create object placement traverser: %w", t, err)
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

				err := f(nodeDesc{local: isLocal, info: addr})

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
		_, err = t.iteratePlacement(f)
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
