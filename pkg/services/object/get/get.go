package getsvc

import (
	"context"
	"errors"
	"fmt"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// Get serves a request to get an object by address, and returns Streamer instance.
func (s *Service) Get(ctx context.Context, prm Prm) error {
	pi, err := checkECPartInfoRequest(prm.common.XHeaders(), prm.container)
	if err != nil {
		// TODO: track https://github.com/nspcc-dev/neofs-api/issues/269.
		return fmt.Errorf("invalid request: %w", err)
	}

	if pi.RuleIndex >= 0 {
		// TODO: deny if node is not in the container?

		if prm.rng != nil {
			if prm.payloadOnly && prm.localGetBuffer != nil {
				stream, err := s.localObjects.ReadECPartRange(ctx, prm.addr.Container(), prm.addr.Object(), pi, prm.rng.GetOffset(), prm.rng.GetLength(), prm.localGetBuffer)
				if err == nil {
					prm.submitLocalGetStreamFn(0, stream)
				}
				return err
			}
			return s.copyLocalECPartRange(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), pi, prm.rng.GetOffset(), prm.rng.GetLength())
		}

		if prm.localGetBuffer != nil {
			n, stream, err := s.localObjects.ReadECPart(ctx, prm.addr.Container(), prm.addr.Object(), pi, prm.localGetBuffer)
			if err == nil {
				prm.submitLocalGetStreamFn(n, stream)
			}
			return err
		}

		return s.copyLocalECPart(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), pi)
	}

	if prm.common.LocalOnly() &&
		len(prm.container.PlacementPolicy().ECRules()) == 0 && // EC breaks TTL requirements currently.
		len(prm.container.PlacementPolicy().Replicas()) != 0 {
		opts := []execOption{withPayloadRange(prm.rng), withPayloadOnly(prm.payloadOnly)}
		if prm.rng == nil && !prm.payloadOnly {
			opts = append(opts, withLocalGetBuffer(prm.localGetBuffer, prm.submitLocalGetStreamFn))
		}
		return s.get(ctx, prm.commonPrm, opts...).err // It handles locality internally.
	}

	nodeLists, repRules, ecRules, err := s.neoFSNet.GetNodesForObject(prm.addr)
	if err != nil {
		return fmt.Errorf("get nodes for object: %w", err)
	}

	if len(repRules) > 0 { // REP format does not require encoding
		opts := []execOption{
			withPreSortedContainerNodes(nodeLists[:len(repRules)], repRules),
			withPayloadRange(prm.rng),
			withPayloadOnly(prm.payloadOnly),
			withForwardGetRequestFunc(prm.forwardRequestFn),
		}
		if prm.rng == nil && !prm.payloadOnly {
			opts = append(opts, withLocalGetBuffer(prm.localGetBuffer, prm.submitLocalGetStreamFn))
		}
		err := s.get(ctx, prm.commonPrm, opts...).err
		if len(ecRules) == 0 || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return err
		}
	}

	ecNodeLists := nodeLists[len(repRules):]
	if prm.forwardRequestFn != nil && !localNodeInSets(s.neoFSNet, ecNodeLists) {
		return s.forwardGetRequest(ctx, ecNodeLists, prm.forwardRequestFn, prm.submitLocalGetStreamFn)
	}

	if prm.raw {
		repRules = make([]uint, len(ecRules))
		for i := range ecRules {
			repRules[i] = uint(ecRules[i].DataPartNum + ecRules[i].ParityPartNum)
		}
		return s.get(ctx, prm.commonPrm, withPreSortedContainerNodes(ecNodeLists, repRules), withPayloadRange(prm.rng), withPayloadOnly(prm.payloadOnly)).err
	}

	if prm.rng != nil {
		hdr, err := s.getECObjectHeader(ctx, prm.addr.Container(), prm.addr.Object(), prm.common.SessionToken(), ecRules, ecNodeLists, nil, nil)
		if err != nil {
			return err
		}
		if err := validatePayloadRange(&hdr, prm.rng); err != nil {
			return err
		}
		hdr = *hdr.CutPayload()
		if prm.payloadOnly {
			if v, ok := prm.objWriter.(HeaderValidator); ok {
				if err := v.ValidateHeader(&hdr); err != nil {
					return err
				}
			}
		} else if err := prm.objWriter.WriteHeader(&hdr); err != nil {
			return err
		}
		return s.copyECObjectRange(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), prm.common.SessionToken(),
			ecRules, ecNodeLists, prm.rng.GetOffset(), prm.rng.GetLength())
	}

	return s.copyECObject(ctx, prm.addr.Container(), prm.addr.Object(), prm.common.SessionToken(),
		ecRules, ecNodeLists, prm.objWriter, prm.ecTransport)
}

// GetRange serves a request to get an object by address, and returns Streamer instance.
func (s *Service) GetRange(ctx context.Context, prm RangePrm) error {
	pi, err := checkECPartInfoRequest(prm.common.XHeaders(), prm.container)
	if err != nil {
		// TODO: track https://github.com/nspcc-dev/neofs-api/issues/269.
		return fmt.Errorf("invalid request: %w", err)
	}

	if pi.RuleIndex >= 0 {
		// TODO: deny if node is not in the container?

		if prm.localBuffer != nil {
			stream, err := s.localObjects.ReadECPartRange(ctx, prm.addr.Container(), prm.addr.Object(), pi, prm.rng.GetOffset(), prm.rng.GetLength(), prm.localBuffer)
			if err == nil {
				prm.submitLocalStreamFn(stream)
			}
			return err
		}

		return s.copyLocalECPartRange(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), pi, prm.rng.GetOffset(), prm.rng.GetLength())
	}

	if prm.common.LocalOnly() &&
		len(prm.container.PlacementPolicy().ECRules()) == 0 && // EC breaks TTL requirements currently.
		len(prm.container.PlacementPolicy().Replicas()) != 0 {
		// It handles locality internally.
		bufOpt := withLocalRangeBuffer(prm.localBuffer, prm.submitLocalStreamFn)
		return s.get(ctx, prm.commonPrm, withPayloadRange(prm.rng), withPayloadOnly(true), withLegacyRange(true), bufOpt).err
	}

	nodeLists, repRules, ecRules, err := s.neoFSNet.GetNodesForObject(prm.addr)
	if err != nil {
		return fmt.Errorf("get nodes for object: %w", err)
	}

	return s.getRange(ctx, prm, nodeLists, repRules, ecRules)
}

func (s *Service) getRange(ctx context.Context, prm RangePrm, nodeLists [][]netmap.NodeInfo, repRules []uint, ecRules []iec.Rule) error {
	if len(repRules) > 0 { // REP format does not require encoding
		bufOpt := withLocalRangeBuffer(prm.localBuffer, prm.submitLocalStreamFn)
		forwardOpt := withForwardRangeRequestFunc(prm.forwardRequestFn)
		err := s.get(ctx, prm.commonPrm, withPreSortedContainerNodes(nodeLists[:len(repRules)], repRules), withPayloadRange(prm.rng), withPayloadOnly(true), withLegacyRange(true), bufOpt, forwardOpt).err
		if len(ecRules) == 0 || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return err
		}
	}

	ecNodeLists := nodeLists[len(repRules):]
	if prm.forwardRequestFn != nil && !localNodeInSets(s.neoFSNet, ecNodeLists) {
		return s.forwardRangeRequest(ctx, ecNodeLists, prm.forwardRequestFn)
	}

	if prm.raw {
		repRules = make([]uint, len(ecRules))
		for i := range ecRules {
			repRules[i] = uint(ecRules[i].DataPartNum + ecRules[i].ParityPartNum)
		}
		return s.get(ctx, prm.commonPrm, withPreSortedContainerNodes(ecNodeLists, repRules), withPayloadRange(prm.rng), withPayloadOnly(true), withLegacyRange(true)).err
	}

	return s.copyECObjectRange(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), prm.common.SessionToken(),
		ecRules, ecNodeLists, prm.rng.GetOffset(), prm.rng.GetLength())
}

func validatePayloadRange(hdr *object.Object, rng *object.Range) error {
	if rng == nil {
		return nil
	}

	off := rng.GetOffset()
	ln := rng.GetLength()
	pldLen := hdr.PayloadSize()

	if ln == 0 {
		if off == 0 {
			return nil
		}
		return apistatus.ErrObjectOutOfRange
	}
	if off >= pldLen || pldLen-off < ln {
		return apistatus.ErrObjectOutOfRange
	}
	return nil
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

	if len(repRules) > 0 {
		headOpt := headOnly(prm.forwardHeadRequestFn, prm.submitHeadResponseFn)
		err := s.get(ctx, prm.commonPrm, headOpt, withPreSortedContainerNodes(nodeLists[:len(repRules)], repRules)).err
		if len(ecRules) == 0 || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return err
		}
	}

	ecNodeLists := nodeLists[len(repRules):]
	if prm.forwardHeadRequestFn != nil && !localNodeInSets(s.neoFSNet, ecNodeLists) {
		return s.forwardHeadRequest(ctx, ecNodeLists, prm.forwardHeadRequestFn, prm.submitHeadResponseFn)
	}

	if prm.raw {
		repRules = make([]uint, len(ecRules))
		for i := range ecRules {
			repRules[i] = uint(ecRules[i].DataPartNum + ecRules[i].ParityPartNum)
		}
		headOpt := headOnly(prm.forwardHeadRequestFn, prm.submitHeadResponseFn)
		return s.get(ctx, prm.commonPrm, headOpt, withPreSortedContainerNodes(ecNodeLists, repRules)).err
	}

	return s.copyECObjectHeader(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), prm.common.SessionToken(),
		ecRules, ecNodeLists, prm.buffer, prm.submitLenFn)
}

func (s *Service) get(ctx context.Context, prm commonPrm, opts ...execOption) statusError {
	exec := &execCtx{
		svc: s,
		ctx: ctx,
		prm: RangePrm{
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
