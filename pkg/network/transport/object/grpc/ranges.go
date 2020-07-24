package object

import (
	"context"
	"io"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	_range "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc/range"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// Range is a type alias of
	// Range from object package of neofs-api-go.
	Range = object.Range

	// RangeDescriptor is a type alias of
	// RangeDescriptor from objio package.
	RangeDescriptor = _range.RangeDescriptor

	// RangeChopper is a type alias of
	// RangeChopper from objio package.
	RangeChopper = _range.RangeChopper

	// GetRangeRequest is a type alias of
	// GetRangeRequest from object package of neofs-api-go.
	GetRangeRequest = object.GetRangeRequest

	// GetRangeResponse is a type alias of
	// GetRangeResponse from object package of neofs-api-go.
	GetRangeResponse = object.GetRangeResponse

	// GetRangeHashRequest is a type alias of
	// GetRangeResponse from object package of neofs-api-go.
	GetRangeHashRequest = object.GetRangeHashRequest

	// GetRangeHashResponse is a type alias of
	// GetRangeHashResponse from object package of neofs-api-go.
	GetRangeHashResponse = object.GetRangeHashResponse

	objectRangeReceiver interface {
		getRange(context.Context, rangeTool) (interface{}, error)
	}

	rangeTool interface {
		transport.RangeHashInfo
		budOff(*RangeDescriptor) rangeTool
		handler() rangeItemAccumulator
	}

	rawRangeInfo struct {
		*rawAddrInfo
		rng Range
	}

	rawRangeHashInfo struct {
		*rawAddrInfo
		rngList []Range
		salt    []byte
	}

	coreRangeReceiver struct {
		rngRevealer     rangeRevealer
		straightRngRecv objectRangeReceiver

		// Set of errors that won't be converted into errPayloadRangeNotFound
		mErr map[error]struct{}

		log *zap.Logger
	}

	straightRangeReceiver struct {
		executor operationExecutor
	}

	singleItemHandler struct {
		*sync.Once
		item interface{}
	}

	rangeItemAccumulator interface {
		responseItemHandler
		collect() (interface{}, error)
	}

	rangeHashAccum struct {
		concat bool
		h      []Hash
	}

	rangeRevealer interface {
		reveal(context.Context, *RangeDescriptor) ([]RangeDescriptor, error)
	}

	coreRngRevealer struct {
		relativeRecv _range.RelativeReceiver
		chopTable    _range.ChopperTable
	}

	getRangeServerWriter struct {
		req *GetRangeRequest

		srv object.Service_GetRangeServer

		respPreparer responsePreparer
	}
)

const (
	emGetRangeFail    = "could get object range #%d part #%d"
	emRangeRevealFail = "could not reveal object range #%d"
	emRangeCollect    = "could not collect result of object range #%d"
)

var errRangeReveal = errors.New("could not reveal payload range")

func (s *objectService) GetRange(req *GetRangeRequest, srv object.Service_GetRangeServer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error(panicLogMsg,
				zap.Stringer("request", object.RequestRange),
				zap.Any("reason", r),
			)

			err = errServerPanic
		}

		err = s.statusCalculator.make(requestError{
			t: object.RequestRange,
			e: err,
		})
	}()

	var r interface{}

	if r, err = s.requestHandler.handleRequest(srv.Context(), handleRequestParams{
		request:  req,
		executor: s,
	}); err == nil {
		_, err = io.CopyBuffer(
			&getRangeServerWriter{
				req:          req,
				srv:          srv,
				respPreparer: s.rangeChunkPreparer,
			},
			r.(io.Reader),
			make([]byte, maxGetPayloadSize),
		)
	}

	return err
}

func (s *objectService) GetRangeHash(ctx context.Context, req *GetRangeHashRequest) (res *GetRangeHashResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error(panicLogMsg,
				zap.Stringer("request", object.RequestRangeHash),
				zap.Any("reason", r),
			)

			err = errServerPanic
		}

		err = s.statusCalculator.make(requestError{
			t: object.RequestRangeHash,
			e: err,
		})
	}()

	var r interface{}

	if r, err = s.requestHandler.handleRequest(ctx, handleRequestParams{
		request:  req,
		executor: s,
	}); err != nil {
		return
	}

	res = makeRangeHashResponse(r.([]Hash))
	err = s.respPreparer.prepareResponse(ctx, req, res)

	return
}

func (s *coreRangeReceiver) getRange(ctx context.Context, rt rangeTool) (res interface{}, err error) {
	defer func() {
		if err != nil {
			if _, ok := s.mErr[errors.Cause(err)]; !ok {
				s.log.Error("get range failure",
					zap.String("error", err.Error()),
				)

				err = errPayloadRangeNotFound
			}
		}
	}()

	var (
		subRngSet []RangeDescriptor
		rngSet    = rt.GetRanges()
		addr      = rt.GetAddress()
		handler   = rt.handler()
	)

	for i := range rngSet {
		rd := RangeDescriptor{
			Size:   int64(rngSet[i].Length),
			Offset: int64(rngSet[i].Offset),
			Addr:   addr,
		}

		if rt.GetTTL() < service.NonForwardingTTL {
			subRngSet = []RangeDescriptor{rd}
		} else if subRngSet, err = s.rngRevealer.reveal(ctx, &rd); err != nil {
			return nil, errors.Wrapf(err, emRangeRevealFail, i+1)
		} else if len(subRngSet) == 0 {
			return nil, errRangeReveal
		}

		subRangeTool := rt.budOff(&rd)
		subHandler := subRangeTool.handler()

		for j := range subRngSet {
			tool := subRangeTool.budOff(&subRngSet[j])

			if subRngSet[j].Addr.Equal(&addr) {
				res, err = s.straightRngRecv.getRange(ctx, tool)
			} else {
				res, err = s.getRange(ctx, tool)
			}

			if err != nil {
				return nil, errors.Wrapf(err, emGetRangeFail, i+1, j+1)
			}

			subHandler.handleItem(res)
		}

		rngRes, err := subHandler.collect()
		if err != nil {
			return nil, errors.Wrapf(err, emRangeCollect, i+1)
		}

		handler.handleItem(rngRes)
	}

	return handler.collect()
}

func (s *straightRangeReceiver) getRange(ctx context.Context, rt rangeTool) (interface{}, error) {
	handler := newSingleItemHandler()
	if err := s.executor.executeOperation(ctx, rt, handler); err != nil {
		return nil, err
	}

	return handler.collect()
}

func (s *coreRngRevealer) reveal(ctx context.Context, r *RangeDescriptor) ([]RangeDescriptor, error) {
	chopper, err := s.getChopper(r.Addr)
	if err != nil {
		return nil, err
	}

	return chopper.Chop(ctx, r.Size, r.Offset, true)
}

func (s *coreRngRevealer) getChopper(addr Address) (res RangeChopper, err error) {
	if res, err = s.chopTable.GetChopper(addr, _range.RCCharybdis); err == nil && res.Closed() {
		return
	} else if res, err = s.chopTable.GetChopper(addr, _range.RCScylla); err == nil {
		return
	} else if res, err = _range.NewScylla(&_range.ChopperParams{
		RelativeReceiver: s.relativeRecv,
		Addr:             addr,
	}); err != nil {
		return nil, err
	}

	_ = s.chopTable.PutChopper(addr, res)

	return
}

func loopData(data []byte, size, off int64) []byte {
	if len(data) == 0 {
		return make([]byte, 0)
	}

	res := make([]byte, 0, size)

	var (
		cut  int64
		tail = data[off%int64(len(data)):]
	)

	for added := int64(0); added < size; added += cut {
		cut = min(int64(len(tail)), size-added)
		res = append(res, tail[:cut]...)
		tail = data
	}

	return res
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

func newSingleItemHandler() rangeItemAccumulator { return &singleItemHandler{Once: new(sync.Once)} }

func (s *singleItemHandler) handleItem(item interface{}) { s.Do(func() { s.item = item }) }

func (s *singleItemHandler) collect() (interface{}, error) { return s.item, nil }

func (s *rangeHashAccum) handleItem(h interface{}) {
	if v, ok := h.(Hash); ok {
		s.h = append(s.h, v)
		return
	}

	s.h = append(s.h, h.([]Hash)...)
}

func (s *rangeHashAccum) collect() (interface{}, error) {
	if s.concat {
		return hash.Concat(s.h)
	}

	return s.h, nil
}

func (s *rawRangeHashInfo) GetRanges() []Range {
	return s.rngList
}

func (s *rawRangeHashInfo) setRanges(v []Range) {
	s.rngList = v
}

func (s *rawRangeHashInfo) GetSalt() []byte {
	return s.salt
}

func (s *rawRangeHashInfo) setSalt(v []byte) {
	s.salt = v
}

func (s *rawRangeHashInfo) getAddrInfo() *rawAddrInfo {
	return s.rawAddrInfo
}

func (s *rawRangeHashInfo) setAddrInfo(v *rawAddrInfo) {
	s.rawAddrInfo = v
	s.setType(object.RequestRangeHash)
}

func newRawRangeHashInfo() *rawRangeHashInfo {
	res := new(rawRangeHashInfo)

	res.setAddrInfo(newRawAddressInfo())

	return res
}

func (s *rawRangeHashInfo) budOff(r *RangeDescriptor) rangeTool {
	res := newRawRangeHashInfo()

	res.setMetaInfo(s.getMetaInfo())
	res.setAddress(r.Addr)
	res.setRanges([]Range{
		{
			Offset: uint64(r.Offset),
			Length: uint64(r.Size),
		},
	})
	res.setSalt(loopData(s.salt, int64(len(s.salt)), r.Offset))
	res.setSessionToken(s.GetSessionToken())
	res.setBearerToken(s.GetBearerToken())
	res.setExtendedHeaders(s.ExtendedHeaders())

	return res
}

func (s *rawRangeHashInfo) handler() rangeItemAccumulator { return &rangeHashAccum{concat: true} }

func (s *transportRequest) GetRanges() []Range {
	return s.serviceRequest.(*object.GetRangeHashRequest).Ranges
}

func (s *transportRequest) GetSalt() []byte {
	return s.serviceRequest.(*object.GetRangeHashRequest).Salt
}

func (s *transportRequest) budOff(rd *RangeDescriptor) rangeTool {
	res := newRawRangeHashInfo()

	res.setTTL(s.GetTTL())
	res.setTimeout(s.GetTimeout())
	res.setAddress(rd.Addr)
	res.setRanges([]Range{
		{
			Offset: uint64(rd.Offset),
			Length: uint64(rd.Size),
		},
	})
	res.setSalt(s.serviceRequest.(*object.GetRangeHashRequest).GetSalt())
	res.setSessionToken(s.GetSessionToken())
	res.setBearerToken(s.GetBearerToken())
	res.setExtendedHeaders(s.ExtendedHeaders())

	return res
}

func (s *transportRequest) handler() rangeItemAccumulator { return new(rangeHashAccum) }

func (s *getRangeServerWriter) Write(p []byte) (int, error) {
	resp := makeRangeResponse(p)
	if err := s.respPreparer.prepareResponse(s.srv.Context(), s.req, resp); err != nil {
		return 0, err
	}

	if err := s.srv.Send(resp); err != nil {
		return 0, err
	}

	return len(p), nil
}

func (s *rawRangeInfo) GetRange() Range {
	return s.rng
}

func (s *rawRangeInfo) setRange(rng Range) {
	s.rng = rng
}

func (s *rawRangeInfo) getAddrInfo() *rawAddrInfo {
	return s.rawAddrInfo
}

func (s *rawRangeInfo) setAddrInfo(v *rawAddrInfo) {
	s.rawAddrInfo = v
	s.setType(object.RequestRange)
}

func newRawRangeInfo() *rawRangeInfo {
	res := new(rawRangeInfo)

	res.setAddrInfo(newRawAddressInfo())

	return res
}

func (s *transportRequest) GetRange() Range {
	return s.serviceRequest.(*GetRangeRequest).Range
}
