package putsvc

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/pkg/errors"
)

type distributedTarget struct {
	traverseOpts []placement.Option

	workerPool util.WorkerPool

	obj *object.RawObject

	chunks [][]byte

	nodeTargetInitializer func(*network.Address) transformer.ObjectTarget
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
		append(t.traverseOpts, placement.ForObject(t.obj.GetID()))...,
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
					// TODO: log error
					return
				} else if _, err := target.Close(); err != nil {
					// TODO: log error
					return
				}

				traverser.SubmitSuccess()
			}); err != nil {
				wg.Done()
				// TODO: log error
				break loop
			}
		}

		wg.Wait()
	}

	if !traverser.Success() {
		return nil, errIncompletePut
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(t.obj.GetID()), nil
}
