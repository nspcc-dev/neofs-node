package putsvc

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type Streamer struct {
	*cfg

	ctx context.Context

	target internal.Target

	relay func(client.NodeInfo, client.MultiAddressClient) error

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

	homomorphicChecksumRequired := !prm.cnr.IsHomomorphicHashingDisabled()

	if prm.hdr.Signature() != nil {
		p.relay = prm.relay

		// prepare untrusted-Put object target
		p.target = &validatingTarget{
			nextTarget: p.newCommonTarget(prm),
			fmt:        p.fmtValidator,

			maxPayloadSz: p.maxPayloadSz,

			homomorphicChecksumRequired: homomorphicChecksumRequired,
		}

		return nil
	}

	sToken := prm.common.SessionToken()

	// prepare trusted-Put object target

	// get private token from local storage
	var sessionInfo *util.SessionInfo

	if sToken != nil {
		sessionInfo = &util.SessionInfo{
			ID:    sToken.ID(),
			Owner: sToken.Issuer(),
		}
	}

	sessionKey, err := p.keyStorage.GetKey(sessionInfo)
	if err != nil {
		return fmt.Errorf("(%T) could not receive session key: %w", p, err)
	}

	signer := neofsecdsa.SignerRFC6979(*sessionKey)

	// In case session token is missing, the line above returns the default key.
	// If it isn't owner key, replication attempts will fail, thus this check.
	if sToken == nil {
		ownerObj := prm.hdr.OwnerID()
		if ownerObj == nil {
			return errors.New("missing object owner")
		}

		ownerSession := user.ResolveFromECDSAPublicKey(signer.PublicKey)

		if !ownerObj.Equals(ownerSession) {
			return fmt.Errorf("(%T) session token is missing but object owner id is different from the default key", p)
		}
	}

	p.target = &validatingTarget{
		fmt:              p.fmtValidator,
		unpreparedObject: true,
		nextTarget: newSlicingTarget(
			p.ctx,
			p.maxPayloadSz,
			!homomorphicChecksumRequired,
			user.NewAutoIDSigner(*sessionKey),
			sToken,
			p.networkState.CurrentEpoch(),
			p.newCommonTarget(prm),
		),
		homomorphicChecksumRequired: homomorphicChecksumRequired,
	}

	return nil
}

func (p *Streamer) preparePrm(prm *PutInitPrm) error {
	var err error

	// get latest network map
	nm, err := netmap.GetLatestNetworkMap(p.netMapSrc)
	if err != nil {
		return fmt.Errorf("(%T) could not get latest network map: %w", p, err)
	}

	idCnr, ok := prm.hdr.ContainerID()
	if !ok {
		return errors.New("missing container ID")
	}

	// get container to store the object
	cnrInfo, err := p.cnrSrc.Get(idCnr)
	if err != nil {
		return fmt.Errorf("(%T) could not get container by ID: %w", p, err)
	}

	prm.cnr = cnrInfo.Value

	// add common options
	prm.traverseOpts = append(prm.traverseOpts,
		// set processing container
		placement.ForContainer(prm.cnr),
	)

	if id, ok := prm.hdr.ID(); ok {
		prm.traverseOpts = append(prm.traverseOpts,
			// set identifier of the processing object
			placement.ForObject(id),
		)
	}

	prm.traverseOpts = append(prm.traverseOpts, placement.WithCopiesNumber(prm.copiesNumber))

	// create placement builder from network map
	builder := placement.NewNetworkMapBuilder(nm)

	if prm.common.LocalOnly() {
		// restrict success count to 1 stored copy (to local storage)
		prm.traverseOpts = append(prm.traverseOpts, placement.SuccessAfter(1))

		// use local-only placement builder
		builder = util.NewLocalPlacement(builder, p.netmapKeys)
	}

	// set placement builder
	prm.traverseOpts = append(prm.traverseOpts, placement.UseBuilder(builder))

	return nil
}

func (p *Streamer) newCommonTarget(prm *PutInitPrm) internal.Target {
	var relay func(nodeDesc) error
	if p.relay != nil {
		relay = func(node nodeDesc) error {
			var info client.NodeInfo

			client.NodeInfoFromNetmapElement(&info, node.info)

			c, err := p.clientConstructor.Get(info)
			if err != nil {
				return fmt.Errorf("could not create SDK client %s: %w", info.AddressGroup(), err)
			}

			return p.relay(info, c)
		}
	}

	// enable additional container broadcast on non-local operation
	// if object has TOMBSTONE or LOCK type.
	typ := prm.hdr.Type()
	withBroadcast := !prm.common.LocalOnly() && (typ == object.TypeTombstone || typ == object.TypeLock)

	return &distributedTarget{
		traversalState: traversal{
			opts: prm.traverseOpts,

			extraBroadcastEnabled: withBroadcast,
		},
		payload:    getPayload(),
		remotePool: p.remotePool,
		localPool:  p.localPool,
		nodeTargetInitializer: func(node nodeDesc) preparedObjectTarget {
			if node.local {
				return &localTarget{
					storage: p.localStore,
				}
			}

			rt := &remoteTarget{
				ctx:               p.ctx,
				keyStorage:        p.keyStorage,
				commonPrm:         prm.common,
				clientConstructor: p.clientConstructor,
			}

			client.NodeInfoFromNetmapElement(&rt.nodeInfo, node.info)

			return rt
		},
		relay: relay,
		fmt:   p.fmtValidator,
		log:   p.log,

		isLocalKey: p.netmapKeys.IsLocalKey,
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

	id, err := p.target.Close()
	if err != nil {
		return nil, fmt.Errorf("(%T) could not close object target: %w", p, err)
	}

	return &PutResponse{
		id: id,
	}, nil
}
