package searchsvc

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

type Streamer struct {
	*cfg

	once *sync.Once

	prm *Prm

	traverser *placement.Traverser

	ctx context.Context

	ch chan []*object.ID

	cache [][]*object.ID
}

func (p *Streamer) Recv() (*Response, error) {
	var err error

	p.once.Do(func() {
		if err = p.preparePrm(p.prm); err == nil {
			go p.start(p.prm)
		}
	})

	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not start streaming", p)
	}

	select {
	case <-p.ctx.Done():
		return nil, errors.Wrapf(p.ctx.Err(), "(%T) context is done", p)
	case v, ok := <-p.ch:
		if !ok {
			return nil, io.EOF
		}

		v = p.cutCached(v)

		return &Response{
			idList: v,
		}, nil
	}
}

func (p *Streamer) cutCached(ids []*object.ID) []*object.ID {
loop:
	for i := 0; i < len(ids); i++ {
		for j := range p.cache {
			for k := range p.cache[j] {
				if ids[i].Equal(p.cache[j][k]) {
					ids = append(ids[:i], ids[i+1:]...)

					i--

					continue loop
				}
			}
		}
	}

	if len(ids) > 0 {
		p.cache = append(p.cache, ids)
	}

	return ids
}

func (p *Streamer) preparePrm(prm *Prm) error {
	var err error

	// get latest network map
	nm, err := netmap.GetLatestNetworkMap(p.netMapSrc)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get latest network map", p)
	}

	// get container to store the object
	cnr, err := p.cnrSrc.Get(prm.cid)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get container by ID", p)
	}

	// allocate placement traverser options
	traverseOpts := make([]placement.Option, 0, 4)

	// add common options
	traverseOpts = append(traverseOpts,
		// set processing container
		placement.ForContainer(cnr),
	)

	// create placement builder from network map
	builder := placement.NewNetworkMapBuilder(nm)

	if prm.common.LocalOnly() {
		// restrict success count to 1 stored copy (to local storage)
		traverseOpts = append(traverseOpts, placement.SuccessAfter(1))

		// use local-only placement builder
		builder = util.NewLocalPlacement(builder, p.localAddrSrc)
	}

	// set placement builder
	traverseOpts = append(traverseOpts, placement.UseBuilder(builder))

	// build placement traverser
	if p.traverser, err = placement.NewTraverser(traverseOpts...); err != nil {
		return errors.Wrapf(err, "(%T) could not build placement traverser", p)
	}

	p.ch = make(chan []*object.ID)

	return nil
}

func (p *Streamer) start(prm *Prm) {
	defer close(p.ch)

loop:
	for {
		addrs := p.traverser.Next()
		if len(addrs) == 0 {
			break
		}

		wg := new(sync.WaitGroup)

		for i := range addrs {
			wg.Add(1)

			addr := addrs[i]

			if err := p.workerPool.Submit(func() {
				defer wg.Done()

				var streamer interface {
					stream(context.Context, chan<- []*object.ID) error
				}

				if network.IsLocalAddress(p.localAddrSrc, addr) {
					streamer = &localStream{
						query:   prm.query,
						storage: p.localStore,
					}
				} else {
					streamer = &remoteStream{
						prm:  prm,
						key:  p.key,
						addr: addr,
					}
				}

				if err := streamer.stream(p.ctx, p.ch); err != nil {
					// TODO: log error
				}
			}); err != nil {
				wg.Done()
				// TODO: log error
				break loop
			}
		}

		wg.Wait()
	}
}
