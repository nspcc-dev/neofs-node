package putsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/pkg/errors"
)

type Streamer struct {
	*cfg

	ctx context.Context

	target transformer.ObjectTarget
}

var errNotInit = errors.New("stream not initialized")

var errInitRecall = errors.New("init recall")

func (p *Streamer) Init(prm *PutInitPrm) error {
	// initialize destination target
	if err := p.initTarget(prm); err != nil {
		return errors.Wrapf(err, "(%T) could not initialize object target", p)
	}

	return errors.Wrapf(
		p.target.WriteHeader(prm.hdr),
		"(%T) could not write header to target", p,
	)
}

func (p *Streamer) initTarget(prm *PutInitPrm) error {
	// prevent re-calling
	if p.target != nil {
		return errInitRecall
	}

	// prepare needed put parameters
	if err := p.preparePrm(prm); err != nil {
		return errors.Wrapf(err, "(%T) could not prepare put parameters", p)
	}

	if prm.hdr.Signature() != nil {
		// prepare untrusted-Put object target
		p.target = &validatingTarget{
			nextTarget: p.newCommonTarget(prm),
			fmt:        p.fmtValidator,
		}

		return nil
	}

	sToken := prm.common.SessionToken()

	// prepare trusted-Put object target

	// get private token from local storage
	sessionKey, err := p.keyStorage.GetKey(sToken)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not receive session key", p)
	}

	maxSz := p.maxSizeSrc.MaxObjectSize()
	if maxSz == 0 {
		return errors.Errorf("(%T) could not obtain max object size parameter", p)
	}

	p.target = transformer.NewPayloadSizeLimiter(
		maxSz,
		func() transformer.ObjectTarget {
			return transformer.NewFormatTarget(&transformer.FormatterParams{
				Key:          sessionKey,
				NextTarget:   p.newCommonTarget(prm),
				SessionToken: sToken,
				NetworkState: p.networkState,
			})
		},
	)

	return nil
}

func (p *Streamer) preparePrm(prm *PutInitPrm) error {
	var err error

	// get latest network map
	nm, err := netmap.GetLatestNetworkMap(p.netMapSrc)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get latest network map", p)
	}

	// get container to store the object
	cnr, err := p.cnrSrc.Get(prm.hdr.ContainerID())
	if err != nil {
		return errors.Wrapf(err, "(%T) could not get container by ID", p)
	}

	// add common options
	prm.traverseOpts = append(prm.traverseOpts,
		// set processing container
		placement.ForContainer(cnr),

		// set identifier of the processing object
		placement.ForObject(prm.hdr.ID()),
	)

	// create placement builder from network map
	builder := placement.NewNetworkMapBuilder(nm)

	if prm.common.LocalOnly() {
		// restrict success count to 1 stored copy (to local storage)
		prm.traverseOpts = append(prm.traverseOpts, placement.SuccessAfter(1))

		// use local-only placement builder
		builder = util.NewLocalPlacement(builder, p.localAddrSrc)
	}

	// set placement builder
	prm.traverseOpts = append(prm.traverseOpts, placement.UseBuilder(builder))

	return nil
}

func (p *Streamer) newCommonTarget(prm *PutInitPrm) transformer.ObjectTarget {
	return &distributedTarget{
		traverseOpts: prm.traverseOpts,
		workerPool:   p.workerPool,
		nodeTargetInitializer: func(addr *network.Address) transformer.ObjectTarget {
			if network.IsLocalAddress(p.localAddrSrc, addr) {
				return &localTarget{
					storage: p.localStore,
				}
			}

			return &remoteTarget{
				ctx:         p.ctx,
				keyStorage:  p.keyStorage,
				commonPrm:   prm.common,
				addr:        addr,
				clientCache: p.clientCache,
			}
		},
		fmt: p.fmtValidator,
		log: p.log,
	}
}

func (p *Streamer) SendChunk(prm *PutChunkPrm) error {
	if p.target == nil {
		return errNotInit
	}

	_, err := p.target.Write(prm.chunk)

	return errors.Wrapf(err, "(%T) could not write payload chunk to target", p)
}

func (p *Streamer) Close() (*PutResponse, error) {
	if p.target == nil {
		return nil, errNotInit
	}

	ids, err := p.target.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not close object target", p)
	}

	id := ids.ParentID()
	if id == nil {
		id = ids.SelfID()
	}

	return &PutResponse{
		id: id,
	}, nil
}
