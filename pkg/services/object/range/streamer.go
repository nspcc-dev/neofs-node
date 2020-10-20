package rangesvc

import (
	"context"
	"io"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/pkg/errors"
)

type Streamer interface {
	Recv() (*Response, error)
}

type streamer struct {
	*cfg

	once *sync.Once

	ctx context.Context

	prm *Prm

	traverser *placement.Traverser

	rangeTraverser *util.RangeTraverser

	ch chan []byte
}

type chunkWriter struct {
	ctx context.Context

	ch chan<- []byte

	written uint64
}

func (p *streamer) Recv() (*Response, error) {
	var err error

	p.once.Do(func() {
		p.ch = make(chan []byte)
		err = p.workerPool.Submit(p.start)
	})

	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not start streaming", p)
	}

	select {
	case <-p.ctx.Done():
		return nil, errors.Wrapf(p.ctx.Err(), "(%T) stream is stopped by context", p)
	case v, ok := <-p.ch:
		if !ok {
			if _, rng := p.rangeTraverser.Next(); rng.GetLength() != 0 {
				return nil, errors.Errorf("(%T) incomplete get payload range", p)
			}

			return nil, io.EOF
		}

		return &Response{
			chunk: v,
		}, nil
	}
}

func (p *streamer) switchToObject(id *object.ID) error {
	var err error

	// get latest network map
	nm, err := netmap.GetLatestNetworkMap(p.netMapSrc)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get latest network map", p)
	}

	// get container to read payload range
	cnr, err := p.cnrSrc.Get(p.prm.addr.GetContainerID())
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get container by ID", p)
	}

	// allocate placement traverser options
	traverseOpts := make([]placement.Option, 0, 4)

	// add common options
	traverseOpts = append(traverseOpts,
		// set processing container
		placement.ForContainer(cnr),

		// set success count (1st incoming full range)
		placement.SuccessAfter(1),

		// set identifier of the processing object
		placement.ForObject(id),
	)

	// create placement builder from network map
	builder := placement.NewNetworkMapBuilder(nm)

	if p.prm.common.LocalOnly() {
		// use local-only placement builder
		builder = util.NewLocalPlacement(builder, p.localAddrSrc)
	}

	// set placement builder
	traverseOpts = append(traverseOpts, placement.UseBuilder(builder))

	// build placement traverser
	if p.traverser, err = placement.NewTraverser(traverseOpts...); err != nil {
		return errors.Wrapf(err, "(%T) could not build placement traverser", p)
	}

	return nil
}

func (p *streamer) start() {
	defer close(p.ch)

	objAddr := object.NewAddress()
	objAddr.SetContainerID(p.prm.addr.GetContainerID())

loop:
	for {
		select {
		case <-p.ctx.Done():
			// TODO: log this
			break loop
		default:
		}

		nextID, nextRange := p.rangeTraverser.Next()
		if nextRange.GetLength() == 0 {
			break
		} else if err := p.switchToObject(nextID); err != nil {
			// TODO: log error
			break
		}

		objAddr.SetObjectID(nextID)

	subloop:
		for {
			select {
			case <-p.ctx.Done():
				// TODO: log this
				break loop
			default:
			}

			addrs := p.traverser.Next()
			if len(addrs) == 0 {
				break
			}

			for i := range addrs {
				wg := new(sync.WaitGroup)
				wg.Add(1)

				addr := addrs[i]

				if err := p.workerPool.Submit(func() {
					defer wg.Done()

					var rngWriter io.WriterTo

					if network.IsLocalAddress(p.localAddrSrc, addr) {
						rngWriter = &localRangeWriter{
							addr:    objAddr,
							rng:     nextRange,
							storage: p.localStore,
						}
					} else {
						rngWriter = &remoteRangeWriter{
							ctx:        p.ctx,
							keyStorage: p.keyStorage,
							node:       addr,
							token:      p.prm.common.SessionToken(),
							bearer:     p.prm.common.BearerToken(),
							addr:       objAddr,
							rng:        nextRange,
						}
					}

					written, err := rngWriter.WriteTo(&chunkWriter{
						ctx: p.ctx,
						ch:  p.ch,
					})
					if err != nil {
						// TODO: log error
					}

					ln := nextRange.GetLength()
					uw := uint64(written)

					p.rangeTraverser.PushSuccessSize(uw)
					nextRange.SetLength(ln - uw)
					nextRange.SetOffset(nextRange.GetOffset() + uw)
				}); err != nil {
					wg.Done()
					// TODO: log error
					break loop
				}

				wg.Wait()

				if nextRange.GetLength() == 0 {
					p.traverser.SubmitSuccess()
					break subloop
				}
			}
		}

		if !p.traverser.Success() {
			// TODO: log error
			break loop
		}
	}
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	case w.ch <- p:
	}

	w.written += uint64(len(p))

	return len(p), nil
}
