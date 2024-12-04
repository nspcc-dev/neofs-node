package putsvc

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
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

	transport Transport
	neoFSNet  NeoFSNetwork
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

		ownerSession := user.NewFromECDSAPublicKey(signer.PublicKey)

		if *ownerObj != ownerSession {
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
	localOnly := prm.common.LocalOnly()
	if localOnly && prm.copiesNumber > 0 {
		return errors.New("storage of multiple object replicas is requested for a local operation")
	}

	localNodeKey, err := p.keyStorage.GetKey(nil)
	if err != nil {
		return fmt.Errorf("get local node's private key: %w", err)
	}

	idCnr := prm.hdr.GetContainerID()
	if idCnr.IsZero() {
		return errors.New("missing container ID")
	}

	// get container to store the object
	cnrInfo, err := p.cnrSrc.Get(idCnr)
	if err != nil {
		return fmt.Errorf("(%T) could not get container by ID: %w", p, err)
	}

	prm.cnr = cnrInfo.Value

	prm.containerNodes, err = p.neoFSNet.GetContainerNodes(idCnr)
	if err != nil {
		return fmt.Errorf("select storage nodes for the container: %w", err)
	}
	cnrNodes := prm.containerNodes.Unsorted()
nextSet:
	for i := range cnrNodes {
		for j := range cnrNodes[i] {
			prm.localNodeInContainer = p.neoFSNet.IsLocalNodePublicKey(cnrNodes[i][j].PublicKey())
			if prm.localNodeInContainer {
				prm.localNodePos[0], prm.localNodePos[1] = i, j
				break nextSet
			}
		}
	}
	if !prm.localNodeInContainer && localOnly {
		return errors.New("local operation on the node not compliant with the container storage policy")
	}

	prm.localNodeSigner = (*neofsecdsa.Signer)(localNodeKey)

	return nil
}

func (p *Streamer) newCommonTarget(prm *PutInitPrm) internal.Target {
	var relay func(nodeDesc) error
	if p.relay != nil {
		relay = func(node nodeDesc) error {
			c, err := p.clientConstructor.Get(node.info)
			if err != nil {
				return fmt.Errorf("could not create SDK client %s: %w", node.info.AddressGroup(), err)
			}

			return p.relay(node.info, c)
		}
	}

	// enable additional container broadcast on non-local operation
	// if object has TOMBSTONE or LOCK type.
	typ := prm.hdr.Type()
	localOnly := prm.common.LocalOnly()
	withBroadcast := !localOnly && (typ == object.TypeTombstone || typ == object.TypeLock)

	return &distributedTarget{
		networkMagicNumber: p.networkMagic,
		placementIterator: placementIterator{
			log:            p.log,
			neoFSNet:       p.neoFSNet,
			localPool:      p.localPool,
			remotePool:     p.remotePool,
			containerNodes: prm.containerNodes,
			localOnly:      localOnly,
			localNodePos:   prm.localNodePos,
			linearReplNum:  uint(prm.copiesNumber),
			broadcast:      withBroadcast,
		},
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
				nodeInfo:          node.info,
				clientConstructor: p.clientConstructor,
				transport:         p.transport,
			}

			return rt
		},
		relay:                relay,
		fmt:                  p.fmtValidator,
		localNodeInContainer: prm.localNodeInContainer,
		localNodeSigner:      prm.localNodeSigner,
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
