package putsvc

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
)

type Streamer struct {
	*cfg

	ctx context.Context

	target transformer.ObjectTarget

	relay func(network.AddressGroup, client.Client) error

	maxPayloadSz uint64 // network config
}

var errNotInit = errors.New("stream not initialized")

var errInitRecall = errors.New("init recall")

func (p *Streamer) Init(prm *PutInitPrm) error {
	// initialize destination target
	if err := p.initTarget(prm); err != nil {
		return fmt.Errorf("(%T) could not initialize object target: %w", p, err)
	}

	if err := p.target.WriteHeader(prm.hdr); err != nil {
		return fmt.Errorf("(%T) could not write header to target: %w", p, err)
	}
	return nil
}

// MaxObjectSize returns maximum payload size for the streaming session.
//
// Must be called after the successful Init.
func (p *Streamer) MaxObjectSize() uint64 {
	return p.maxPayloadSz
}

func (p *Streamer) initTarget(prm *PutInitPrm) error {
	// prevent re-calling
	if p.target != nil {
		return errInitRecall
	}

	// prepare needed put parameters
	if err := p.preparePrm(prm); err != nil {
		return fmt.Errorf("(%T) could not prepare put parameters: %w", p, err)
	}

	p.maxPayloadSz = p.maxSizeSrc.MaxObjectSize()
	if p.maxPayloadSz == 0 {
		return fmt.Errorf("(%T) could not obtain max object size parameter", p)
	}

	if prm.hdr.Signature() != nil {
		p.relay = prm.relay

		// prepare untrusted-Put object target
		p.target = &validatingTarget{
			nextTarget: p.newCommonTarget(prm),
			fmt:        p.fmtValidator,

			maxPayloadSz: p.maxPayloadSz,
		}

		return nil
	}

	sToken := prm.common.SessionToken()

	// prepare trusted-Put object target

	// get private token from local storage
	sessionKey, err := p.keyStorage.GetKey(sToken)
	if err != nil {
		return fmt.Errorf("(%T) could not receive session key: %w", p, err)
	}

	p.target = transformer.NewPayloadSizeLimiter(
		p.maxPayloadSz,
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
		return fmt.Errorf("(%T) could not get latest network map: %w", p, err)
	}

	// get container to store the object
	cnr, err := p.cnrSrc.Get(prm.hdr.ContainerID())
	if err != nil {
		return fmt.Errorf("(%T) could not get container by ID: %w", p, err)
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

var errLocalAddress = errors.New("can't relay to local address")

func (p *Streamer) newCommonTarget(prm *PutInitPrm) transformer.ObjectTarget {
	var relay func(placement.Node) error
	if p.relay != nil {
		relay = func(node placement.Node) error {
			addr := node.Addresses()

			if network.IsLocalAddress(p.localAddrSrc, addr) {
				return errLocalAddress
			}

			c, err := p.clientConstructor.Get(addr)
			if err != nil {
				return fmt.Errorf("could not create SDK client %s: %w", addr, err)
			}

			return p.relay(addr, c)
		}
	}

	return &distributedTarget{
		traverseOpts: prm.traverseOpts,
		workerPool:   p.workerPool,
		nodeTargetInitializer: func(addr network.AddressGroup) transformer.ObjectTarget {
			if network.IsLocalAddress(p.localAddrSrc, addr) {
				return &localTarget{
					storage: p.localStore,
				}
			}

			return &remoteTarget{
				ctx:               p.ctx,
				keyStorage:        p.keyStorage,
				commonPrm:         prm.common,
				addr:              addr,
				clientConstructor: p.clientConstructor,
			}
		},
		relay: relay,
		fmt:   p.fmtValidator,
		log:   p.log,
	}
}

func (p *Streamer) SendChunk(prm *PutChunkPrm) error {
	if p.target == nil {
		return errNotInit
	}

	if _, err := p.target.Write(prm.chunk); err != nil {
		return fmt.Errorf("(%T) could not write payload chunk to target: %w", p, err)
	}

	return nil
}

func (p *Streamer) Close() (*PutResponse, error) {
	if p.target == nil {
		return nil, errNotInit
	}

	ids, err := p.target.Close()
	if err != nil {
		return nil, fmt.Errorf("(%T) could not close object target: %w", p, err)
	}

	id := ids.ParentID()
	if id == nil {
		id = ids.SelfID()
	}

	return &PutResponse{
		id: id,
	}, nil
}
