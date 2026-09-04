package getsvc

import (
	"context"
	"errors"
	"fmt"

	inetmap "github.com/nspcc-dev/neofs-node/internal/netmap"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// Get serves a request to get an object by address, and returns Streamer instance.
func (s *Service) Get(ctx context.Context, prm Prm) error {
	var neofsNet NeoFSNetwork
	// range requests do not support fetching additional info about EC parts
	if !prm.payloadRange.IsSet() {
		neofsNet = s.neoFSNet
	}

	pi, err := checkECPartInfoGetRequest(neofsNet, prm)
	if err != nil {
		// TODO: track https://github.com/nspcc-dev/neofs-api/issues/269.
		return fmt.Errorf("invalid request: %w", err)
	}

	if pi.RuleIndex >= 0 {
		// TODO: deny if node is not in the container?

		if prm.localGetBuffer != nil {
			n, stream, err := s.localObjects.ReadECPart(ctx, prm.addr.Container(), prm.addr.Object(), pi, prm.payloadRange, prm.localGetBuffer, prm.interceptHeaderBinaryFn)
			if err == nil {
				prm.submitLocalGetStreamFn(n, stream)
			}
			return err
		}

		if prm.payloadRange.IsSet() {
			var headerFn func(*object.Object) error
			if !prm.payloadOnly || prm.recheckEACL {
				headerFn = func(hdr *object.Object) error {
					return writeObjectHeader(prm.objWriter, hdr, prm.payloadOnly)
				}
			}
			return s.copyLocalECPartPayloadRange(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), pi, prm.payloadRange, headerFn)
		}

		return s.copyLocalECPart(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), pi, prm.ecReturnAnyPart)
	}

	if prm.common.LocalOnly() &&
		len(prm.container.PlacementPolicy().ECRules()) == 0 && // EC breaks TTL requirements currently.
		len(prm.container.PlacementPolicy().Replicas()) != 0 {
		opts := []execOption{withPayloadRangePrm(prm.payloadRange), withPayloadOnly(prm.payloadOnly), withEACLRecheck(prm.recheckEACL),
			withLocalGetBuffer(prm.localGetBuffer, prm.submitLocalGetStreamFn, prm.interceptHeaderBinaryFn)}
		return s.get(ctx, prm.commonPrm, opts...).err // It handles locality internally.
	}

	nodeLists, repRules, ecRules, err := s.neoFSNet.GetNodesForObject(prm.addr)
	if err != nil {
		return fmt.Errorf("get nodes for object: %w", err)
	}

	if prm.forwardRequestFn != nil && !inetmap.NodeSetsContainPublicKeyFunc(nodeLists, s.neoFSNet.IsLocalNodePublicKey) {
		return s.forwardRequest(ctx, repRules, ecRules, nodeLists, "GET", prm.forwardRequestFn)
	}

	if len(repRules) > 0 { // REP format does not require encoding
		opts := []execOption{
			withPreSortedContainerNodes(nodeLists[:len(repRules)], repRules),
			withPayloadRangePrm(prm.payloadRange),
			withPayloadOnly(prm.payloadOnly),
			withGetTransportFunc(prm.transportFn),
			withEACLRecheck(prm.recheckEACL),
			withLocalGetBuffer(prm.localGetBuffer, prm.submitLocalGetStreamFn, prm.interceptHeaderBinaryFn),
		}
		err := s.get(ctx, prm.commonPrm, opts...).err
		if len(ecRules) == 0 || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return err
		}
	}

	ecNodeLists := nodeLists[len(repRules):]

	if prm.raw {
		repRules = make([]uint, len(ecRules))
		for i := range ecRules {
			repRules[i] = uint(ecRules[i].DataPartNum + ecRules[i].ParityPartNum)
		}
		return s.get(ctx, prm.commonPrm, withPreSortedContainerNodes(ecNodeLists, repRules), withPayloadRangePrm(prm.payloadRange), withPayloadOnly(prm.payloadOnly), withEACLRecheck(prm.recheckEACL)).err
	}

	if prm.payloadRange.IsSet() {
		return s.copyECObjectRange(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), ecRules, ecNodeLists, prm.payloadRange, func(hdr *object.Object) error {
			return writeObjectHeader(prm.objWriter, hdr.CutPayload(), prm.payloadOnly)
		})
	}

	return s.copyECObject(ctx, prm.addr.Container(), prm.addr.Object(), ecRules, ecNodeLists, prm.objWriter, prm.ecTransport)
}

func writeObjectHeader(dst ObjectWriter, hdr *object.Object, payloadOnly bool) error {
	if payloadOnly {
		if v, ok := dst.(HeaderValidator); ok {
			return v.ValidateHeader(hdr)
		}
		return nil
	}

	return dst.WriteHeader(hdr)
}

// Head reads object header from container.
//
// Returns ErrNotFound if the header was not received for the call.
// Returns SplitInfoError if object is virtual and raw flag is set.
func (s *Service) Head(ctx context.Context, prm HeadPrm) error {
	pi, err := checkECPartInfoRequest(prm.common.XHeaders(), prm.container)
	if err != nil {
		// TODO: track https://github.com/nspcc-dev/neofs-api/issues/269.
		return fmt.Errorf("invalid request: %w", err)
	}

	if pi.RuleIndex >= 0 {
		// TODO: deny if node is not in the container?

		if prm.buffer != nil {
			n, err := s.localObjects.ReadECPartHeader(ctx, prm.addr.Container(), prm.addr.Object(), pi, prm.buffer)
			if err == nil {
				prm.submitLenFn(n)
			}
			return err
		}

		return s.copyLocalECPartHeader(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), pi)
	}

	if prm.common.LocalOnly() {
		if prm.buffer != nil {
			n, err := s.localObjects.ReadHeader(ctx, prm.addr, prm.raw, prm.buffer)
			if err == nil {
				prm.submitLenFn(n)
			}
			return err
		}

		return s.copyLocalObjectHeader(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), prm.raw)
	}

	nodeLists, repRules, ecRules, err := s.neoFSNet.GetNodesForObject(prm.addr)
	if err != nil {
		return fmt.Errorf("get nodes for object: %w", err)
	}

	if prm.forwardRequestFn != nil && !inetmap.NodeSetsContainPublicKeyFunc(nodeLists, s.neoFSNet.IsLocalNodePublicKey) {
		return s.forwardRequest(ctx, repRules, ecRules, nodeLists, "HEAD", prm.forwardRequestFn)
	}

	if len(repRules) > 0 {
		transportOpt := headOnly(prm.transportFn, prm.submitHeadResponseFn)
		err := s.get(ctx, prm.commonPrm, transportOpt, withPreSortedContainerNodes(nodeLists[:len(repRules)], repRules)).err
		if len(ecRules) == 0 || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return err
		}
	}

	ecNodeLists := nodeLists[len(repRules):]

	if prm.raw {
		repRules = make([]uint, len(ecRules))
		for i := range ecRules {
			repRules[i] = uint(ecRules[i].DataPartNum + ecRules[i].ParityPartNum)
		}
		headOpt := headOnly(prm.transportFn, prm.submitHeadResponseFn)
		return s.get(ctx, prm.commonPrm, headOpt, withPreSortedContainerNodes(ecNodeLists, repRules)).err
	}

	return s.copyECObjectHeader(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), ecRules, ecNodeLists, prm.buffer, prm.submitLenFn)
}

func (s *Service) get(ctx context.Context, prm commonPrm, opts ...execOption) statusError {
	exec := &execCtx{
		svc: s,
		ctx: ctx,
		prm: rangePrm{
			commonPrm: prm,
		},
		infoSplit: object.NewSplitInfo(),
	}

	for i := range opts {
		opts[i](exec)
	}

	// allow overwriting by explicit option
	if exec.log == nil {
		exec.setLogger(s.log)
	}

	exec.execute() //nolint:contextcheck // It is in fact passed via execCtx

	if exec.collectDst != nil {
		exec.collectDst.hdr = exec.collectedHeader
		exec.collectDst.rc = exec.collectedReader
	}

	return exec.statusError
}

func (exec *execCtx) execute() {
	exec.log.Debug("serving request...")

	// perform local operation
	exec.executeLocal()

	exec.analyzeStatus(true)
}

func (exec *execCtx) analyzeStatus(execCnr bool) {
	// analyze local result
	switch exec.status {
	case statusOK:
		exec.log.Debug("operation finished successfully")
	case statusVIRTUAL:
		exec.log.Debug("requested object is virtual")
		if exec.collectOnly {
			return
		}
		exec.assemble()
		if errors.Is(exec.err, errNoLinkNoLastPart) && execCnr {
			exec.executeOnContainer()
			exec.analyzeStatus(false)
		}
	case statusAPIResponse:
		exec.log.Debug("received api response locally, return directly", zap.Error(exec.err))
		return
	default:
		exec.log.Debug("operation finished with error",
			zap.Error(exec.err),
		)

		if execCnr && !exec.responseStarted {
			exec.executeOnContainer()
			exec.analyzeStatus(false)
		}
	}
}
