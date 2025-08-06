package putsvc

import (
	"context"
	"errors"
	"fmt"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type Streamer struct {
	*Service
}

func (p *Streamer) WriteHeader(ctx context.Context, hdr *object.Object, cp *util.CommonPrm, opts PutInitOptions) (internal.PayloadWriter, error) {
	// initialize destination target
	target, err := p.initTarget(ctx, hdr, cp, opts)
	if err != nil {
		return nil, err
	}

	if err := target.WriteHeader(hdr); err != nil {
		return nil, err
	}

	return target, nil
}

func (p *Streamer) initTarget(ctx context.Context, hdr *object.Object, cp *util.CommonPrm, opts PutInitOptions) (internal.Target, error) {
	// prepare needed put parameters
	if err := p.prepareOptions(hdr, cp, &opts); err != nil {
		return nil, fmt.Errorf("(%T) could not prepare put parameters: %w", p, err)
	}

	maxPayloadSz := p.maxSizeSrc.MaxObjectSize()
	if maxPayloadSz == 0 {
		return nil, fmt.Errorf("(%T) could not obtain max object size parameter", p)
	}

	homomorphicChecksumRequired := !opts.cnr.IsHomomorphicHashingDisabled()

	if hdr.Signature() != nil {
		// prepare untrusted-Put object target
		return &validatingTarget{
			nextTarget: p.newCommonTarget(ctx, cp, opts, opts.relay),
			fmt:        p.fmtValidator,

			maxPayloadSz: maxPayloadSz,

			homomorphicChecksumRequired: homomorphicChecksumRequired,
		}, nil
	}

	sToken := cp.SessionToken()

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
		return nil, fmt.Errorf("(%T) could not receive session key: %w", p, err)
	}

	signer := neofsecdsa.SignerRFC6979(*sessionKey)

	// In case session token is missing, the line above returns the default key.
	// If it isn't owner key, replication attempts will fail, thus this check.
	if sToken == nil {
		ownerObj := hdr.Owner()
		if ownerObj.IsZero() {
			return nil, errors.New("missing object owner")
		}

		ownerSession := user.NewFromECDSAPublicKey(signer.PublicKey)

		if ownerObj != ownerSession {
			return nil, fmt.Errorf("(%T) session token is missing but object owner id is different from the default key", p)
		}
	}

	sessionSigner := user.NewAutoIDSigner(*sessionKey)
	opts.sessionSigner = sessionSigner
	return &validatingTarget{
		fmt:              p.fmtValidator,
		unpreparedObject: true,
		nextTarget: newSlicingTarget(
			ctx,
			maxPayloadSz,
			!homomorphicChecksumRequired,
			sessionSigner,
			sToken,
			p.networkState.CurrentEpoch(),
			p.newCommonTarget(ctx, cp, opts, nil),
		),
		homomorphicChecksumRequired: homomorphicChecksumRequired,
	}, nil
}

func (p *Streamer) prepareOptions(hdr *object.Object, cp *util.CommonPrm, opts *PutInitOptions) error {
	localOnly := cp.LocalOnly()
	if localOnly && opts.copiesNumber > 1 {
		return errors.New("storage of multiple object replicas is requested for a local operation")
	}

	localNodeKey, err := p.keyStorage.GetKey(nil)
	if err != nil {
		return fmt.Errorf("get local node's private key: %w", err)
	}

	idCnr := hdr.GetContainerID()
	if idCnr.IsZero() {
		return errors.New("missing container ID")
	}

	// get container to store the object
	opts.cnr, err = p.cnrSrc.Get(idCnr)
	if err != nil {
		return fmt.Errorf("(%T) could not get container by ID: %w", p, err)
	}

	opts.containerNodes, err = p.neoFSNet.GetContainerNodes(idCnr)
	if err != nil {
		return fmt.Errorf("select storage nodes for the container: %w", err)
	}
	cnrNodes := opts.containerNodes.Unsorted()
	ecRulesN := len(opts.containerNodes.ECRules())
	if ecRulesN > 0 {
		ecPart, err := iec.GetPartInfo(*hdr)
		if err != nil {
			return fmt.Errorf("get EC part info from object header: %w", err)
		}

		repRulesN := len(opts.containerNodes.PrimaryCounts())
		if ecPart.Index >= 0 {
			if ecPart.RuleIndex >= ecRulesN {
				return fmt.Errorf("invalid EC part info in object header: EC rule idx=%d with %d rules in total", ecPart.RuleIndex, ecRulesN)
			}
			if hdr.Signature() == nil {
				return errors.New("unsigned EC part object")
			}
			opts.localNodeInContainer = localNodeInSet(p.neoFSNet, cnrNodes[repRulesN+ecPart.RuleIndex])
		} else {
			if repRulesN == 0 && hdr.Signature() != nil {
				return errors.New("missing EC part info in signed object")
			}
			opts.localNodeInContainer = localNodeInSets(p.neoFSNet, cnrNodes)
		}

		opts.ecPart = ecPart
	} else {
		opts.localNodeInContainer = localNodeInSets(p.neoFSNet, cnrNodes)
	}
	if !opts.localNodeInContainer && localOnly {
		return errors.New("local operation on the node not compliant with the container storage policy")
	}

	opts.localNodeSigner = (*neofsecdsa.Signer)(localNodeKey)
	opts.localSignerRFC6979 = (*neofsecdsa.SignerRFC6979)(localNodeKey)

	return nil
}

func (p *Streamer) newCommonTarget(ctx context.Context, cp *util.CommonPrm, opts PutInitOptions, relayFn RelayFunc) internal.Target {
	var relay func(nodeDesc) error
	if relayFn != nil {
		relay = func(node nodeDesc) error {
			c, err := p.clientConstructor.Get(node.info)
			if err != nil {
				return fmt.Errorf("could not create SDK client %s: %w", node.info.AddressGroup(), err)
			}

			return relayFn(node.info, c)
		}
	}

	return &distributedTarget{
		opCtx:              ctx,
		fsState:            p.networkState,
		networkMagicNumber: p.networkMagic,
		metaSvc:            p.metaSvc,
		placementIterator: placementIterator{
			log:           p.log,
			neoFSNet:      p.neoFSNet,
			remotePool:    p.remotePool,
			linearReplNum: uint(opts.copiesNumber),
		},
		localStorage:            p.localStore,
		keyStorage:              p.keyStorage,
		commonPrm:               cp,
		clientConstructor:       p.clientConstructor,
		transport:               p.transport,
		relay:                   relay,
		fmt:                     p.fmtValidator,
		containerNodes:          opts.containerNodes,
		ecPart:                  opts.ecPart,
		localNodeInContainer:    opts.localNodeInContainer,
		localNodeSigner:         opts.localNodeSigner,
		sessionSigner:           opts.sessionSigner,
		cnrClient:               p.cfg.cnrClient,
		metainfoConsistencyAttr: metaAttribute(opts.cnr),
		metaSigner:              opts.localSignerRFC6979,
		localOnly:               cp.LocalOnly(),
	}
}

func metaAttribute(cnr container.Container) string {
	return cnr.Attribute("__NEOFS__METAINFO_CONSISTENCY")
}
