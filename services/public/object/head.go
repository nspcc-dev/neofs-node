package object

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/objio"
	"github.com/nspcc-dev/neofs-node/lib/transformer"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	objectData struct {
		*Object
		payload io.Reader
	}

	objectReceiver interface {
		getObject(context.Context, ...transport.GetInfo) (*objectData, error)
	}

	rangeDataReceiver interface {
		recvData(context.Context, transport.RangeInfo, io.Writer) error
	}

	rangeReaderAccumulator interface {
		responseItemHandler
		rangeData() io.Reader
	}

	rangeRdrAccum struct {
		*sync.Once
		r io.Reader
	}

	straightRangeDataReceiver struct {
		executor operationExecutor
	}

	coreObjectReceiver struct {
		straightObjRecv objectReceiver
		childLister     objectChildrenLister
		ancestralRecv   ancestralObjectsReceiver

		log *zap.Logger
	}

	straightObjectReceiver struct {
		executor operationExecutor
	}

	objectRewinder interface {
		rewind(context.Context, ...Object) (*Object, error)
	}

	payloadPartReceiver interface {
		recvPayload(context.Context, []transport.RangeInfo) (io.Reader, error)
	}

	corePayloadPartReceiver struct {
		rDataRecv        rangeDataReceiver
		windowController slidingWindowController
	}

	slidingWindowController interface {
		newWindow() (WorkerPool, error)
	}

	simpleWindowController struct {
		windowSize int
	}

	coreObjectRewinder struct {
		transformer transformer.ObjectRestorer
	}

	objectAccumulator interface {
		responseItemHandler
		object() *Object
	}

	coreObjAccum struct {
		*sync.Once
		obj *Object
	}

	rawGetInfo struct {
		*rawAddrInfo
	}

	rawHeadInfo struct {
		rawGetInfo
		fullHeaders bool
	}

	childrenReceiver interface {
		getChildren(context.Context, Address, []ID) ([]Object, error)
	}

	coreChildrenReceiver struct {
		coreObjRecv objectReceiver
		timeout     time.Duration
	}

	payloadRangeReceiver interface {
		getRangeData(context.Context, transport.RangeInfo, ...Object) (io.Reader, error)
	}

	corePayloadRangeReceiver struct {
		chopTable   objio.ChopperTable
		relRecv     objio.RelativeReceiver
		payloadRecv payloadPartReceiver

		// Set of errors that won't be converted to errPayloadRangeNotFound
		mErr map[error]struct{}

		log *zap.Logger
	}

	ancestralObjectsReceiver interface {
		getFromChildren(context.Context, Address, []ID, bool) (*objectData, error)
	}

	coreAncestralReceiver struct {
		childrenRecv childrenReceiver
		objRewinder  objectRewinder
		pRangeRecv   payloadRangeReceiver
		timeout      time.Duration
	}

	emptyReader struct{}
)

const (
	emHeadRecvFail = "could not receive %d of %d object head"

	childrenNotFound = internal.Error("could not find child objects")
	errNonAssembly   = internal.Error("node is not capable to assemble the object")
)

var (
	_ objectReceiver     = (*straightObjectReceiver)(nil)
	_ objectReceiver     = (*coreObjectReceiver)(nil)
	_ objectRewinder     = (*coreObjectRewinder)(nil)
	_ objectAccumulator  = (*coreObjAccum)(nil)
	_ transport.HeadInfo = (*transportRequest)(nil)
	_ transport.HeadInfo = (*rawHeadInfo)(nil)
	_ transport.GetInfo  = (*transportRequest)(nil)
	_ transport.GetInfo  = (*rawGetInfo)(nil)

	_ payloadPartReceiver = (*corePayloadPartReceiver)(nil)

	_ ancestralObjectsReceiver = (*coreAncestralReceiver)(nil)

	_ childrenReceiver = (*coreChildrenReceiver)(nil)

	_ payloadRangeReceiver = (*corePayloadRangeReceiver)(nil)

	_ rangeDataReceiver = (*straightRangeDataReceiver)(nil)

	_ slidingWindowController = (*simpleWindowController)(nil)

	_ io.Reader = (*emptyReader)(nil)

	_ rangeReaderAccumulator = (*rangeRdrAccum)(nil)
)

func (s *objectService) Head(ctx context.Context, req *object.HeadRequest) (res *object.HeadResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error(panicLogMsg,
				zap.Stringer("request", object.RequestHead),
				zap.Any("reason", r),
			)

			err = errServerPanic
		}

		err = s.statusCalculator.make(requestError{
			t: object.RequestHead,
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

	obj := r.(*objectData).Object
	if !req.FullHeaders {
		obj.Headers = nil
	}

	res = makeHeadResponse(obj)
	err = s.respPreparer.prepareResponse(ctx, req, res)

	return res, err
}

func (s *coreObjectReceiver) getObject(ctx context.Context, info ...transport.GetInfo) (*objectData, error) {
	var (
		childCount int
		children   []ID
	)

	obj, err := s.straightObjRecv.getObject(ctx, s.sendingRequest(info[0]))

	if info[0].GetRaw() {
		return obj, err
	} else if err == nil {
		children = obj.Links(object.Link_Child)
		if childCount = len(children); childCount <= 0 {
			return obj, nil
		}
	}

	if s.ancestralRecv == nil {
		return nil, errNonAssembly
	}

	ctx = contextWithValues(ctx,
		transformer.PublicSessionToken, info[0].GetSessionToken(),
		implementations.BearerToken, info[0].GetBearerToken(),
		implementations.ExtendedHeaders, info[0].ExtendedHeaders(),
	)

	if childCount <= 0 {
		if children = s.childLister.children(ctx, info[0].GetAddress()); len(children) == 0 {
			return nil, childrenNotFound
		}
	}

	res, err := s.ancestralRecv.getFromChildren(ctx, info[0].GetAddress(), children, info[0].Type() == object.RequestHead)
	if err != nil {
		s.log.Error("could not get object from children",
			zap.String("error", err.Error()),
		)

		return nil, errIncompleteOperation
	}

	return res, nil
}

func (s *coreObjectReceiver) sendingRequest(src transport.GetInfo) transport.GetInfo {
	if s.ancestralRecv == nil || src.GetRaw() {
		return src
	}

	getInfo := *newRawGetInfo()
	getInfo.setTimeout(src.GetTimeout())
	getInfo.setAddress(src.GetAddress())
	getInfo.setRaw(true)
	getInfo.setSessionToken(src.GetSessionToken())
	getInfo.setBearerToken(src.GetBearerToken())
	getInfo.setExtendedHeaders(src.ExtendedHeaders())
	getInfo.setTTL(
		maxu32(
			src.GetTTL(),
			service.NonForwardingTTL,
		),
	)

	if src.Type() == object.RequestHead {
		headInfo := newRawHeadInfo()
		headInfo.setGetInfo(getInfo)
		headInfo.setFullHeaders(true)

		return headInfo
	}

	return getInfo
}

func (s *coreAncestralReceiver) getFromChildren(ctx context.Context, addr Address, children []ID, head bool) (*objectData, error) {
	var (
		err       error
		childObjs []Object
		res       = new(objectData)
	)

	if childObjs, err = s.childrenRecv.getChildren(ctx, addr, children); err != nil {
		return nil, err
	} else if res.Object, err = s.objRewinder.rewind(ctx, childObjs...); err != nil {
		return nil, err
	}

	if head {
		return res, nil
	}

	rngInfo := newRawRangeInfo()
	rngInfo.setTTL(service.NonForwardingTTL)
	rngInfo.setTimeout(s.timeout)
	rngInfo.setAddress(addr)
	rngInfo.setSessionToken(tokenFromContext(ctx))
	rngInfo.setBearerToken(bearerFromContext(ctx))
	rngInfo.setExtendedHeaders(extendedHeadersFromContext(ctx))
	rngInfo.setRange(Range{
		Length: res.SystemHeader.PayloadLength,
	})

	res.payload, err = s.pRangeRecv.getRangeData(ctx, rngInfo, childObjs...)

	return res, err
}

func (s *corePayloadRangeReceiver) getRangeData(ctx context.Context, info transport.RangeInfo, selection ...Object) (res io.Reader, err error) {
	defer func() {
		if err != nil {
			if _, ok := s.mErr[errors.Cause(err)]; !ok {
				s.log.Error("get payload range data failure",
					zap.String("error", err.Error()),
				)

				err = errPayloadRangeNotFound
			}
		}
	}()

	var (
		chopper RangeChopper
		addr    = info.GetAddress()
	)

	chopper, err = s.chopTable.GetChopper(addr, objio.RCCharybdis)
	if err != nil || !chopper.Closed() {
		if len(selection) == 0 {
			if chopper, err = s.chopTable.GetChopper(addr, objio.RCScylla); err != nil {
				if chopper, err = objio.NewScylla(&objio.ChopperParams{
					RelativeReceiver: s.relRecv,
					Addr:             addr,
				}); err != nil {
					return
				}
			}
		} else {
			rs := make([]RangeDescriptor, 0, len(selection))
			for i := range selection {
				rs = append(rs, RangeDescriptor{
					Size: int64(selection[i].SystemHeader.PayloadLength),
					Addr: *selection[i].Address(),

					LeftBound:  i == 0,
					RightBound: i == len(selection)-1,
				})
			}

			if chopper, err = objio.NewCharybdis(&objio.CharybdisParams{
				Addr:           addr,
				ReadySelection: rs,
			}); err != nil {
				return
			}
		}
	}

	_ = s.chopTable.PutChopper(addr, chopper)

	r := info.GetRange()

	ctx = contextWithValues(ctx,
		transformer.PublicSessionToken, info.GetSessionToken(),
		implementations.BearerToken, info.GetBearerToken(),
		implementations.ExtendedHeaders, info.ExtendedHeaders(),
	)

	var rList []RangeDescriptor

	if rList, err = chopper.Chop(ctx, int64(r.Length), int64(r.Offset), true); err != nil {
		return
	}

	return s.payloadRecv.recvPayload(ctx, newRangeInfoList(info, rList))
}

func newRangeInfoList(src transport.RangeInfo, rList []RangeDescriptor) []transport.RangeInfo {
	var infoList []transport.RangeInfo
	if l := len(rList); l == 1 && src.GetAddress().Equal(&rList[0].Addr) {
		infoList = []transport.RangeInfo{src}
	} else {
		infoList = make([]transport.RangeInfo, 0, l)
		for i := range rList {
			rngInfo := newRawRangeInfo()

			rngInfo.setTTL(src.GetTTL())
			rngInfo.setTimeout(src.GetTimeout())
			rngInfo.setAddress(rList[i].Addr)
			rngInfo.setSessionToken(src.GetSessionToken())
			rngInfo.setBearerToken(src.GetBearerToken())
			rngInfo.setExtendedHeaders(src.ExtendedHeaders())
			rngInfo.setRange(Range{
				Offset: uint64(rList[i].Offset),
				Length: uint64(rList[i].Size),
			})

			infoList = append(infoList, rngInfo)
		}
	}

	return infoList
}

func (s *corePayloadPartReceiver) recvPayload(ctx context.Context, rList []transport.RangeInfo) (io.Reader, error) {
	pool, err := s.windowController.newWindow()
	if err != nil {
		return nil, err
	}

	var (
		readers = make([]io.Reader, 0, len(rList))
		writers = make([]*io.PipeWriter, 0, len(rList))
	)

	for range rList {
		r, w := io.Pipe()
		readers = append(readers, r)
		writers = append(writers, w)
	}

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		for i := range rList {
			select {
			case <-ctx.Done():
				return
			default:
			}

			rd, w := rList[i], writers[i]

			if err := pool.Submit(func() {
				err := s.rDataRecv.recvData(ctx, rd, w)
				if err != nil {
					cancel()
				}
				_ = w.CloseWithError(err)
			}); err != nil {
				_ = w.CloseWithError(err)

				cancel()

				break
			}
		}
	}()

	return io.MultiReader(readers...), nil
}

func (s *simpleWindowController) newWindow() (WorkerPool, error) { return ants.NewPool(s.windowSize) }

func (s *straightRangeDataReceiver) recvData(ctx context.Context, info transport.RangeInfo, w io.Writer) error {
	rAccum := newRangeReaderAccumulator()
	err := s.executor.executeOperation(ctx, info, rAccum)

	if err == nil {
		_, err = io.Copy(w, rAccum.rangeData())
	}

	return err
}

func maxu32(a, b uint32) uint32 {
	if a > b {
		return a
	}

	return b
}

func (s *straightObjectReceiver) getObject(ctx context.Context, info ...transport.GetInfo) (*objectData, error) {
	accum := newObjectAccumulator()
	if err := s.executor.executeOperation(ctx, info[0], accum); err != nil {
		return nil, err
	}

	return &objectData{
		Object:  accum.object(),
		payload: new(emptyReader),
	}, nil
}

func (s *coreChildrenReceiver) getChildren(ctx context.Context, parent Address, children []ID) ([]Object, error) {
	objList := make([]Object, 0, len(children))

	headInfo := newRawHeadInfo()
	headInfo.setTTL(service.NonForwardingTTL)
	headInfo.setTimeout(s.timeout)
	headInfo.setFullHeaders(true)
	headInfo.setSessionToken(tokenFromContext(ctx))
	headInfo.setBearerToken(bearerFromContext(ctx))
	headInfo.setExtendedHeaders(extendedHeadersFromContext(ctx))

	for i := range children {
		headInfo.setAddress(Address{
			ObjectID: children[i],
			CID:      parent.CID,
		})

		obj, err := s.coreObjRecv.getObject(ctx, headInfo)
		if err != nil {
			return nil, errors.Errorf(emHeadRecvFail, i+1, len(children))
		}

		objList = append(objList, *obj.Object)
	}

	return transformer.GetChain(objList...)
}

func tokenFromContext(ctx context.Context) service.SessionToken {
	if v, ok := ctx.Value(transformer.PublicSessionToken).(service.SessionToken); ok {
		return v
	}

	return nil
}

func bearerFromContext(ctx context.Context) service.BearerToken {
	if v, ok := ctx.Value(implementations.BearerToken).(service.BearerToken); ok {
		return v
	}

	return nil
}

func extendedHeadersFromContext(ctx context.Context) []service.ExtendedHeader {
	if v, ok := ctx.Value(implementations.ExtendedHeaders).([]service.ExtendedHeader); ok {
		return v
	}

	return nil
}

func (s *coreObjectRewinder) rewind(ctx context.Context, objs ...Object) (*Object, error) {
	objList, err := s.transformer.Restore(ctx, objs...)
	if err != nil {
		return nil, err
	}

	return &objList[0], nil
}

func (s *coreObjAccum) handleItem(v interface{}) { s.Do(func() { s.obj = v.(*Object) }) }

func (s *coreObjAccum) object() *Object { return s.obj }

func newObjectAccumulator() objectAccumulator { return &coreObjAccum{Once: new(sync.Once)} }

func (s *rawGetInfo) getAddrInfo() *rawAddrInfo {
	return s.rawAddrInfo
}

func (s *rawGetInfo) setAddrInfo(v *rawAddrInfo) {
	s.rawAddrInfo = v
	s.setType(object.RequestGet)
}

func newRawGetInfo() *rawGetInfo {
	res := new(rawGetInfo)

	res.setAddrInfo(newRawAddressInfo())

	return res
}

func (s rawHeadInfo) GetFullHeaders() bool {
	return s.fullHeaders
}

func (s *rawHeadInfo) setFullHeaders(v bool) {
	s.fullHeaders = v
}

func (s rawHeadInfo) getGetInfo() rawGetInfo {
	return s.rawGetInfo
}

func (s *rawHeadInfo) setGetInfo(v rawGetInfo) {
	s.rawGetInfo = v
	s.setType(object.RequestHead)
}

func newRawHeadInfo() *rawHeadInfo {
	res := new(rawHeadInfo)

	res.setGetInfo(*newRawGetInfo())

	return res
}

func (s *transportRequest) GetAddress() Address {
	switch t := s.serviceRequest.(type) {
	case *object.HeadRequest:
		return t.Address
	case *GetRangeRequest:
		return t.Address
	case *object.GetRangeHashRequest:
		return t.Address
	case *object.DeleteRequest:
		return t.Address
	case *object.GetRequest:
		return t.Address
	default:
		panic(fmt.Sprintf(pmWrongRequestType, t))
	}
}

func (s *transportRequest) GetFullHeaders() bool {
	return s.serviceRequest.(*object.HeadRequest).GetFullHeaders()
}

func (s *transportRequest) Raw() bool {
	return s.serviceRequest.GetRaw()
}

func (s *emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

func newRangeReaderAccumulator() rangeReaderAccumulator { return &rangeRdrAccum{Once: new(sync.Once)} }

func (s *rangeRdrAccum) rangeData() io.Reader { return s.r }

func (s *rangeRdrAccum) handleItem(r interface{}) { s.Do(func() { s.r = r.(io.Reader) }) }
