package headsvc

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/pkg/errors"
)

type distributedHeader struct {
	*cfg

	w *onceHeaderWriter

	traverser *placement.Traverser
}

func (h *distributedHeader) head(ctx context.Context, prm *Prm) (*Response, error) {
	if err := h.prepare(ctx, prm); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not prepare parameters", h)
	}

	return h.finish(ctx, prm)
}

func (h *distributedHeader) prepare(ctx context.Context, prm *Prm) error {
	var err error

	// get latest network map
	nm, err := netmap.GetLatestNetworkMap(h.netMapSrc)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get latest network map", h)
	}

	// get container to store the object
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

		// set success count (1st incoming header)
		placement.SuccessAfter(1),

		// set identifier of the processing object
		placement.ForObject(prm.addr.GetObjectID()),
	)

	// create placement builder from network map
	builder := placement.NewNetworkMapBuilder(nm)

	if prm.local {
		// use local-only placement builder
		builder = util.NewLocalPlacement(placement.NewNetworkMapBuilder(nm), h.localAddrSrc)
	}

	// set placement builder
	traverseOpts = append(traverseOpts, placement.UseBuilder(builder))

	// build placement traverser
	if h.traverser, err = placement.NewTraverser(traverseOpts...); err != nil {
		return errors.Wrapf(err, "(%T) could not build placement traverser", h)
	}

	return nil
}

func (h *distributedHeader) finish(ctx context.Context, prm *Prm) (*Response, error) {
	resp := new(Response)

	h.w = &onceHeaderWriter{
		once:      new(sync.Once),
		traverser: h.traverser,
		resp:      resp,
	}

	ctx, h.w.cancel = context.WithCancel(ctx)

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

				var header interface {
					head(context.Context, *Prm, func(*object.Object)) error
				}

				if network.IsLocalAddress(h.localAddrSrc, addr) {
					header = &localHeader{
						storage: h.localStore,
					}
				} else {
					header = &remoteHeader{
						key:  h.key,
						node: addr,
					}
				}

				if err := header.head(ctx, prm, h.w.write); err != nil {
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
		return nil, errors.Errorf("(%T) incomplete object Head operation", h)
	}

	return resp, nil
}
