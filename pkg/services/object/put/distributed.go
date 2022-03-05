package putsvc

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	svcutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

type distributedTarget struct {
	traversal traversal

	remotePool, localPool util.WorkerPool

	obj *objectSDK.Object

	chunks [][]byte

	nodeTargetInitializer func(nodeDesc) transformer.ObjectTarget

	isLocalKey func([]byte) bool

	relay func(nodeDesc) error

	fmt *object.FormatValidator

	log *logger.Logger
}

// parameters and state of container traversal.
type traversal struct {
	opts []placement.Option

	// need of additional broadcast after the object is saved
	extraBroadcastEnabled bool

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
		if x.mExclude == nil {
			x.mExclude = make(map[string]struct{}, 1)
		}

		x.mExclude[string(n.PublicKey())] = struct{}{}
	}
}

// checks if specified node was processed during the primary object placement.
func (x traversal) processed(n placement.Node) bool {
	_, ok := x.mExclude[string(n.PublicKey())]
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
	t.chunks = append(t.chunks, p)

	return len(p), nil
}

func (t *distributedTarget) Close() (*transformer.AccessIdentifiers, error) {
	sz := 0

	for i := range t.chunks {
		sz += len(t.chunks[i])
	}

	payload := make([]byte, 0, sz)

	for i := range t.chunks {
		payload = append(payload, t.chunks[i]...)
	}

	t.obj.SetPayload(payload)

	if err := t.fmt.ValidateContent(t.obj); err != nil {
		return nil, fmt.Errorf("(%T) could not validate payload content: %w", t, err)
	}

	return t.iteratePlacement(t.sendObject)
}

func (t *distributedTarget) sendObject(node nodeDesc) error {
	if !node.local && t.relay != nil {
		return t.relay(node)
	}

	target := t.nodeTargetInitializer(node)

	if err := target.WriteHeader(t.obj); err != nil {
		return fmt.Errorf("could not write header: %w", err)
	} else if _, err := target.Close(); err != nil {
		return fmt.Errorf("could not close object stream: %w", err)
	}
	return nil
}

func (t *distributedTarget) iteratePlacement(f func(nodeDesc) error) (*transformer.AccessIdentifiers, error) {
	traverser, err := placement.NewTraverser(
		append(t.traversal.opts, placement.ForObject(t.obj.ID()))...,
	)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not create object placement traverser: %w", t, err)
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

		return nil, err
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

	return new(transformer.AccessIdentifiers).
		WithSelfID(t.obj.ID()), nil
}
