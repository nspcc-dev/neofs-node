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

func (p *Service) InitPut(ctx context.Context, hdr *object.Object, cp *util.CommonPrm, opts PutInitOptions) (internal.PayloadWriter, error) {
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

func (p *Service) initTarget(ctx context.Context, hdr *object.Object, cp *util.CommonPrm, opts PutInitOptions) (internal.Target, error) {
	localOnly := cp.LocalOnly()
	if localOnly && opts.CopiesNumber > 1 {
		return nil, errors.New("storage of multiple object replicas is requested for a local operation")
	}

	localNodeKey, err := p.keyStorage.GetKey(nil)
	if err != nil {
		return nil, fmt.Errorf("get local node's private key: %w", err)
	}

	idCnr := hdr.GetContainerID()
	if idCnr.IsZero() {
		return nil, errors.New("missing container ID")
	}

	// get container to store the object
	cnr, err := p.cnrSrc.Get(idCnr)
	if err != nil {
		return nil, fmt.Errorf("(%T) could not get container by ID: %w", p, err)
	}

	containerNodes, err := p.neoFSNet.GetContainerNodes(idCnr)
	if err != nil {
		return nil, fmt.Errorf("select storage nodes for the container: %w", err)
	}

	cnrNodes := containerNodes.Unsorted()
	ecRulesN := len(containerNodes.ECRules())

	var localNodeInContainer bool
	var ecPart iec.PartInfo
	if ecRulesN > 0 {
		ecPart, err = iec.GetPartInfo(*hdr)
		if err != nil {
			return nil, fmt.Errorf("get EC part info from object header: %w", err)
		}

		repRulesN := len(containerNodes.PrimaryCounts())
		if ecPart.Index >= 0 {
			if ecPart.RuleIndex >= ecRulesN {
				return nil, fmt.Errorf("invalid EC part info in object header: EC rule idx=%d with %d rules in total", ecPart.RuleIndex, ecRulesN)
			}
			if hdr.Signature() == nil {
				return nil, errors.New("unsigned EC part object")
			}
			localNodeInContainer = localNodeInSet(p.neoFSNet, cnrNodes[repRulesN+ecPart.RuleIndex])
		} else {
			if repRulesN == 0 && hdr.Signature() != nil {
				return nil, errors.New("missing EC part info in signed object")
			}
			localNodeInContainer = localNodeInSets(p.neoFSNet, cnrNodes)
		}
	} else {
		localNodeInContainer = localNodeInSets(p.neoFSNet, cnrNodes)
	}
	if !localNodeInContainer && localOnly {
		return nil, errors.New("local operation on the node not compliant with the container storage policy")
	}

	maxPayloadSz := p.maxSizeSrc.MaxObjectSize()
	if maxPayloadSz == 0 {
		return nil, fmt.Errorf("(%T) could not obtain max object size parameter", p)
	}

	homomorphicChecksumRequired := !cnr.IsHomomorphicHashingDisabled()

	target := &distributedTarget{
		svc:                     p,
		localNodeSigner:         (*neofsecdsa.Signer)(localNodeKey),
		metaSigner:              (*neofsecdsa.SignerRFC6979)(localNodeKey),
		opCtx:                   ctx,
		commonPrm:               cp,
		localOnly:               cp.LocalOnly(),
		linearReplNum:           uint(opts.CopiesNumber),
		metainfoConsistencyAttr: metaAttribute(cnr),
		containerNodes:          containerNodes,
		localNodeInContainer:    localNodeInContainer,
		ecPart:                  ecPart,
	}

	if hdr.Signature() != nil {
		// prepare untrusted-Put object target
		if opts.Relay != nil {
			target.relay = func(node nodeDesc) error {
				c, err := p.clientConstructor.Get(node.info)
				if err != nil {
					return fmt.Errorf("could not create SDK client %s: %w", node.info.AddressGroup(), err)
				}

				return opts.Relay(node.info, c)
			}
		}
		return &validatingTarget{
			nextTarget: target,
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
	target.sessionSigner = sessionSigner
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
			target,
		),
		homomorphicChecksumRequired: homomorphicChecksumRequired,
	}, nil
}

func metaAttribute(cnr container.Container) string {
	return cnr.Attribute("__NEOFS__METAINFO_CONSISTENCY")
}
