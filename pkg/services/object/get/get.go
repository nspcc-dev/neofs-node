package getsvc

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// Get serves a request to get an object by address, and returns Streamer instance.
func (s *Service) Get(ctx context.Context, prm Prm) error {
	pi, err := checkECPartInfoRequest(prm.common.XHeaders())
	if err != nil {
		// TODO: track https://github.com/nspcc-dev/neofs-api/issues/269.
		return fmt.Errorf("invalid request: %w", err)
	}

	nodeLists, repRules, ecRules, err := s.neoFSNet.GetNodesForObject(prm.addr)
	if err != nil {
		return fmt.Errorf("get nodes for object: %w", err)
	}

	if pi.RuleIndex >= 0 {
		if err := checkPartRequestAgainstPolicy(ecRules, pi); err != nil {
			// TODO: track https://github.com/nspcc-dev/neofs-api/issues/269.
			return fmt.Errorf("invalid request: %w", err)
		}
		// TODO: deny if node is not in the container?
		return s.copyLocalECPart(prm.objWriter, prm.addr.Container(), prm.addr.Object(), pi)
	}

	if len(repRules) > 0 { // REP format does not require encoding
		err := s.get(ctx, prm.commonPrm, withPreSortedContainerNodes(nodeLists[:len(repRules)], repRules)).err
		if len(ecRules) == 0 || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return err
		}
	}

	return s.copyECObject(ctx, prm.addr.Container(), prm.addr.Object(), prm.common.SessionToken(),
		ecRules, nodeLists[len(repRules):], prm.objWriter)
}

// GetRange serves a request to get an object by address, and returns Streamer instance.
func (s *Service) GetRange(ctx context.Context, prm RangePrm) error {
	pi, err := checkECPartInfoRequest(prm.common.XHeaders())
	if err != nil {
		// TODO: track https://github.com/nspcc-dev/neofs-api/issues/269.
		return fmt.Errorf("invalid request: %w", err)
	}

	nodeLists, repRules, ecRules, err := s.neoFSNet.GetNodesForObject(prm.addr)
	if err != nil {
		return fmt.Errorf("get nodes for object: %w", err)
	}

	if pi.RuleIndex >= 0 {
		if err := checkPartRequestAgainstPolicy(ecRules, pi); err != nil {
			// TODO: track https://github.com/nspcc-dev/neofs-api/issues/269.
			return fmt.Errorf("invalid request: %w", err)
		}
		// TODO: deny if node is not in the container?
		return s.copyLocalECPartRange(prm.objWriter, prm.addr.Container(), prm.addr.Object(), pi, prm.rng.GetOffset(), prm.rng.GetLength())
	}

	if len(repRules) > 0 { // REP format does not require encoding
		err := s.get(ctx, prm.commonPrm, withPreSortedContainerNodes(nodeLists[:len(repRules)], repRules), withPayloadRange(prm.rng)).err
		if len(ecRules) == 0 || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return err
		}
	}

	return s.copyECObjectRange(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), prm.common.SessionToken(),
		ecRules, nodeLists[len(repRules):], prm.rng.GetOffset(), prm.rng.GetLength())
}

func (s *Service) getRange(ctx context.Context, prm RangePrm, opts ...execOption) error {
	return s.get(ctx, prm.commonPrm, append(opts, withPayloadRange(prm.rng))...).err
}

func (s *Service) GetRangeHash(ctx context.Context, prm RangeHashPrm) (*RangeHashRes, error) {
	hashes := make([][]byte, 0, len(prm.rngs))

	for _, rng := range prm.rngs {
		h := prm.hashGen()

		// For big ranges we could fetch range-hashes from different nodes and concatenate them locally.
		// However,
		// 1. Potential gains are insignificant when operating in the Internet given typical latencies and losses.
		// 2. Parallel solution is more complex in terms of code.
		// 3. TZ-hash is likely to be disabled in private installations.
		rngPrm := RangePrm{
			commonPrm: prm.commonPrm,
		}

		rngPrm.SetRange(&rng)
		rngPrm.SetChunkWriter(&hasherWrapper{
			hash: util.NewSaltingWriter(h, prm.salt),
		})

		if err := s.getRange(ctx, rngPrm, withHash(&prm)); err != nil {
			return nil, err
		}

		if prm.forwardedRangeHashResponse != nil {
			// forwarder request case; no need to collect the other
			// parts, the whole response has already been received
			hashes = prm.forwardedRangeHashResponse
			break
		}

		hashes = append(hashes, h.Sum(nil))
	}

	return &RangeHashRes{
		hashes: hashes,
	}, nil
}

// Head reads object header from container.
//
// Returns ErrNotFound if the header was not received for the call.
// Returns SplitInfoError if object is virtual and raw flag is set.
func (s *Service) Head(ctx context.Context, prm HeadPrm) error {
	nodeLists, repRules, ecRules, err := s.neoFSNet.GetNodesForObject(prm.addr)
	if err != nil {
		return fmt.Errorf("get nodes for object: %w", err)
	}

	if prm.common.LocalOnly() {
		return s.copyLocalObjectHeader(prm.objWriter, prm.addr.Container(), prm.addr.Object(), prm.raw)
	}

	if len(repRules) > 0 {
		err := s.get(ctx, prm.commonPrm, headOnly(), withPreSortedContainerNodes(nodeLists[:len(repRules)], repRules)).err
		if len(ecRules) == 0 || !errors.Is(err, apistatus.ErrObjectNotFound) {
			return err
		}
	}

	return s.copyECObjectHeader(ctx, prm.objWriter, prm.addr.Container(), prm.addr.Object(), prm.common.SessionToken(),
		ecRules, nodeLists[len(repRules):])
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

		if execCnr {
			exec.executeOnContainer()
			exec.analyzeStatus(false)
		}
	}
}
