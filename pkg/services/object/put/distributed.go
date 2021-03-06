package putsvc

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	svcutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
)

type distributedTarget struct {
	traverseOpts []placement.Option

	workerPool util.WorkerPool

	obj *object.RawObject

	chunks [][]byte

	nodeTargetInitializer func(*network.Address) transformer.ObjectTarget

	fmt *object.FormatValidator

	log *logger.Logger
}

var errIncompletePut = errors.New("incomplete object put")

func (t *distributedTarget) WriteHeader(obj *object.RawObject) error {
	t.obj = obj

	return nil
}

func (t *distributedTarget) Write(p []byte) (n int, err error) {
	t.chunks = append(t.chunks, p)

	return len(p), nil
}

func (t *distributedTarget) Close() (*transformer.AccessIdentifiers, error) {
	traverser, err := placement.NewTraverser(
		append(t.traverseOpts, placement.ForObject(t.obj.ID()))...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not create object placement traverser", t)
	}

	sz := 0

	for i := range t.chunks {
		sz += len(t.chunks[i])
	}

	payload := make([]byte, 0, sz)

	for i := range t.chunks {
		payload = append(payload, t.chunks[i]...)
	}

	t.obj.SetPayload(payload)

	if err := t.fmt.ValidateContent(t.obj.Object()); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not validate payload content", t)
	}

loop:
	for {
		addrs := traverser.Next()
		if len(addrs) == 0 {
			break
		}

		wg := new(sync.WaitGroup)

		for i := range addrs {
			wg.Add(1)

			addr := addrs[i]

			if err := t.workerPool.Submit(func() {
				defer wg.Done()

				target := t.nodeTargetInitializer(addr)

				if err := target.WriteHeader(t.obj); err != nil {
					svcutil.LogServiceError(t.log, "PUT", addr,
						errors.Wrap(err, "could not write header"))

					return
				} else if _, err := target.Close(); err != nil {
					svcutil.LogServiceError(t.log, "PUT", addr,
						errors.Wrap(err, "could not close object stream"))

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
		return nil, errIncompletePut
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(t.obj.ID()), nil
}
