package putsvc

import (
	"errors"
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	svcutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

type distributedTarget struct {
	traverseOpts []placement.Option

	workerPool util.WorkerPool

	obj *object.RawObject

	chunks [][]byte

	nodeTargetInitializer func(network.AddressGroup) transformer.ObjectTarget

	relay func(network.AddressGroup) error

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
		return nil, fmt.Errorf("(%T) could not validate payload content: %w", t, err)
	}

	return t.iteratePlacement(t.sendObject)
}

func (t *distributedTarget) sendObject(addr network.AddressGroup) error {
	if t.relay != nil {
		err := t.relay(addr)
		if err == nil || !errors.Is(err, errLocalAddress) {
			return err
		}
	}

	target := t.nodeTargetInitializer(addr)

	if err := target.WriteHeader(t.obj); err != nil {
		return fmt.Errorf("could not write header: %w", err)
	} else if _, err := target.Close(); err != nil {
		return fmt.Errorf("could not close object stream: %w", err)
	}
	return nil
}

func (t *distributedTarget) iteratePlacement(f func(network.AddressGroup) error) (*transformer.AccessIdentifiers, error) {
	traverser, err := placement.NewTraverser(
		append(t.traverseOpts, placement.ForObject(t.obj.ID()))...,
	)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not create object placement traverser: %w", t, err)
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

				if err := f(network.GroupFromAddress(addr)); err != nil {
					svcutil.LogServiceError(t.log, "PUT", addr, err)
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
