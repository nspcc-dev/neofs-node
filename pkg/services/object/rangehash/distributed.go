package rangehashsvc

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/pkg/errors"
)

type distributedHasher struct {
	*cfg

	traverser *placement.Traverser
}

func (h *distributedHasher) head(ctx context.Context, prm *Prm) (*Response, error) {
	if err := h.prepare(ctx, prm); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not prepare parameters", h)
	}

	return h.finish(ctx, prm)
}

func (h *distributedHasher) prepare(ctx context.Context, prm *Prm) error {
	var err error

	// get latest network map
	nm, err := netmap.GetLatestNetworkMap(h.netMapSrc)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get latest network map", h)
	}

	// get container to read the object
	cnr, err := h.cnrSrc.Get(prm.addr.GetContainerID())
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get container by ID", h)
	}

	// allocate placement traverser options
	traverseOpts := make([]placement.Option, 0, 4)

	// add common options
	traverseOpts = append(traverseOpts,
		// set processing container
		placement.ForContainer(cnr),

		// set success count (1st incoming hashes)
		placement.SuccessAfter(1),

		// set identifier of the processing object
		placement.ForObject(prm.addr.GetObjectID()),
	)

	// create placement builder from network map
	builder := placement.NewNetworkMapBuilder(nm)

	if prm.common.LocalOnly() {
		// use local-only placement builder
		builder = util.NewLocalPlacement(builder, h.localAddrSrc)
	}

	// set placement builder
	traverseOpts = append(traverseOpts, placement.UseBuilder(builder))

	// build placement traverser
	if h.traverser, err = placement.NewTraverser(traverseOpts...); err != nil {
		return errors.Wrapf(err, "(%T) could not build placement traverser", h)
	}

	return nil
}

func (h *distributedHasher) finish(ctx context.Context, prm *Prm) (*Response, error) {
	resp := new(Response)

	w := &onceHashWriter{
		once:      new(sync.Once),
		traverser: h.traverser,
		resp:      resp,
	}

	ctx, w.cancel = context.WithCancel(ctx)

loop:
	for {
		addrs := h.traverser.Next()
		if len(addrs) == 0 {
			break
		}

		wg := new(sync.WaitGroup)

		for i := range addrs {
			wg.Add(1)

			addr := addrs[i]

			if err := h.workerPool.Submit(func() {
				defer wg.Done()

				var hasher interface {
					hashRange(context.Context, *Prm, func([][]byte)) error
				}

				if network.IsLocalAddress(h.localAddrSrc, addr) {
					hasher = &localHasher{
						storage: h.localStore,
					}
				} else {
					hasher = &remoteHasher{
						key:  h.key,
						node: addr,
					}
				}

				if err := hasher.hashRange(ctx, prm, w.write); err != nil {
					// TODO: log error
					return
				}
			}); err != nil {
				wg.Done()
				// TODO: log error
				break loop
			}
		}

		wg.Wait()
	}

	if !h.traverser.Success() {
		return nil, errors.Errorf("(%T) incomplete object GetRangeHash operation", h)
	}

	return resp, nil
}
