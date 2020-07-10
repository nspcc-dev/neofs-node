package object

import (
	"bytes"
	"context"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/placement"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	operationExecutor interface {
		executeOperation(context.Context, transport.MetaInfo, responseItemHandler) error
	}

	coreOperationExecutor struct {
		pre executionParamsComputer
		fin operationFinalizer
		loc operationExecutor
	}

	operationFinalizer interface {
		completeExecution(context.Context, operationParams) error
	}

	computableParams struct {
		addr               Address
		stopCount          int
		allowPartialResult bool
		tryPreviousNetMap  bool
		selfForward        bool
		maxRecycleCount    int
		reqType            object.RequestType
	}

	responseItemHandler interface {
		handleItem(interface{})
	}

	operationParams struct {
		computableParams
		metaInfo    transport.MetaInfo
		itemHandler responseItemHandler
	}

	coreOperationFinalizer struct {
		curPlacementBuilder  placementBuilder
		prevPlacementBuilder placementBuilder
		interceptorPreparer  interceptorPreparer
		workerPool           WorkerPool
		traverseExec         implementations.ContainerTraverseExecutor
		resLogger            resultLogger
		log                  *zap.Logger
	}

	localFullObjectReceiver interface {
		getObject(context.Context, Address) (*Object, error)
	}

	localHeadReceiver interface {
		headObject(context.Context, Address) (*Object, error)
	}

	localObjectStorer interface {
		putObject(context.Context, *Object) error
	}

	localQueryImposer interface {
		imposeQuery(context.Context, CID, []byte, int) ([]Address, error)
	}

	localRangeReader interface {
		getRange(context.Context, Address, Range) ([]byte, error)
	}

	localRangeHasher interface {
		getHashes(context.Context, Address, []Range, []byte) ([]Hash, error)
	}

	localStoreExecutor struct {
		salitor    Salitor
		epochRecv  EpochReceiver
		localStore localstore.Localstore
	}

	localOperationExecutor struct {
		objRecv   localFullObjectReceiver
		headRecv  localHeadReceiver
		objStore  localObjectStorer
		queryImp  localQueryImposer
		rngReader localRangeReader
		rngHasher localRangeHasher
	}

	coreHandler struct {
		traverser   containerTraverser
		itemHandler responseItemHandler
		resLogger   resultLogger
		reqType     object.RequestType
	}

	executionParamsComputer interface {
		computeParams(*computableParams, transport.MetaInfo)
	}

	coreExecParamsComp struct{}

	resultTracker interface {
		trackResult(context.Context, resultItems)
	}

	interceptorPreparer interface {
		prepareInterceptor(interceptorItems) (func(context.Context, multiaddr.Multiaddr) bool, error)
	}

	interceptorItems struct {
		selfForward bool
		handler     transport.ResultHandler
		metaInfo    transport.MetaInfo
		itemHandler responseItemHandler
	}

	coreInterceptorPreparer struct {
		localExec    operationExecutor
		addressStore implementations.AddressStore
	}

	resultItems struct {
		requestType  object.RequestType
		node         multiaddr.Multiaddr
		satisfactory bool
	}

	idleResultTracker struct {
	}

	resultLogger interface {
		logErr(object.RequestType, multiaddr.Multiaddr, error)
	}

	coreResultLogger struct {
		mLog map[object.RequestType]struct{}
		log  *zap.Logger
	}
)

const (
	errIncompleteOperation = internal.Error("operation is not completed")

	emRangeReadFail = "could not read %d range data"
)

var (
	_ resultTracker           = (*idleResultTracker)(nil)
	_ executionParamsComputer = (*coreExecParamsComp)(nil)
	_ operationFinalizer      = (*coreOperationFinalizer)(nil)
	_ operationExecutor       = (*localOperationExecutor)(nil)
	_ operationExecutor       = (*coreOperationExecutor)(nil)
	_ transport.ResultHandler = (*coreHandler)(nil)
	_ localFullObjectReceiver = (*localStoreExecutor)(nil)
	_ localHeadReceiver       = (*localStoreExecutor)(nil)
	_ localObjectStorer       = (*localStoreExecutor)(nil)
	_ localRangeReader        = (*localStoreExecutor)(nil)
	_ localRangeHasher        = (*localStoreExecutor)(nil)
	_ resultLogger            = (*coreResultLogger)(nil)
)

func (s *coreExecParamsComp) computeParams(p *computableParams, req transport.MetaInfo) {
	switch p.reqType = req.Type(); p.reqType {
	case object.RequestPut:
		if req.GetTTL() < service.NonForwardingTTL {
			p.stopCount = 1
		} else {
			p.stopCount = int(req.(transport.PutInfo).CopiesNumber())
		}

		p.allowPartialResult = false
		p.tryPreviousNetMap = false
		p.selfForward = false
		p.addr = *req.(transport.PutInfo).GetHead().Address()
		p.maxRecycleCount = 0
	case object.RequestGet:
		p.stopCount = 1
		p.allowPartialResult = false
		p.tryPreviousNetMap = true
		p.selfForward = false
		p.addr = req.(transport.AddressInfo).GetAddress()
		p.maxRecycleCount = 0
	case object.RequestHead:
		p.stopCount = 1
		p.allowPartialResult = false
		p.tryPreviousNetMap = true
		p.selfForward = false
		p.addr = req.(transport.AddressInfo).GetAddress()
		p.maxRecycleCount = 0
	case object.RequestSearch:
		p.stopCount = -1 // to traverse all possible nodes in current and prev container
		p.allowPartialResult = true
		p.tryPreviousNetMap = true
		p.selfForward = false
		p.addr = Address{CID: req.(transport.SearchInfo).GetCID()}
		p.maxRecycleCount = 0
	case object.RequestRange:
		p.stopCount = 1
		p.allowPartialResult = false
		p.tryPreviousNetMap = false
		p.selfForward = false
		p.addr = req.(transport.AddressInfo).GetAddress()
		p.maxRecycleCount = 0
	case object.RequestRangeHash:
		p.stopCount = 1
		p.allowPartialResult = false
		p.tryPreviousNetMap = false
		p.selfForward = false
		p.addr = req.(transport.AddressInfo).GetAddress()
		p.maxRecycleCount = 0
	}
}

func (s idleResultTracker) trackResult(context.Context, resultItems) {}

func (s *coreOperationExecutor) executeOperation(ctx context.Context, req transport.MetaInfo, h responseItemHandler) error {
	// if TTL is zero then execute local operation
	if req.GetTTL() < service.NonForwardingTTL {
		return s.loc.executeOperation(ctx, req, h)
	}

	p := new(computableParams)
	s.pre.computeParams(p, req)

	return s.fin.completeExecution(ctx, operationParams{
		computableParams: *p,
		metaInfo:         req,
		itemHandler:      h,
	})
}

func (s *coreOperationFinalizer) completeExecution(ctx context.Context, p operationParams) error {
	traverser := newContainerTraverser(&traverseParams{
		tryPrevNM:            p.tryPreviousNetMap,
		addr:                 p.addr,
		curPlacementBuilder:  s.curPlacementBuilder,
		prevPlacementBuilder: s.prevPlacementBuilder,
		maxRecycleCount:      p.maxRecycleCount,
		stopCount:            p.stopCount,
	})

	handler := &coreHandler{
		traverser:   traverser,
		itemHandler: p.itemHandler,
		resLogger:   s.resLogger,
		reqType:     p.reqType,
	}

	interceptor, err := s.interceptorPreparer.prepareInterceptor(interceptorItems{
		selfForward: p.selfForward,
		handler:     handler,
		metaInfo:    p.metaInfo,
		itemHandler: p.itemHandler,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.traverseExec.Execute(ctx, implementations.TraverseParams{
		TransportInfo:        p.metaInfo,
		Handler:              handler,
		Traverser:            traverser,
		WorkerPool:           s.workerPool,
		ExecutionInterceptor: interceptor,
	})

	switch err := errors.Cause(traverser.Err()); err {
	case container.ErrNotFound:
		return &detailedError{
			error: errContainerNotFound,
			d:     containerDetails(p.addr.CID, descContainerNotFound),
		}
	case placement.ErrEmptyNodes:
		if !p.allowPartialResult {
			return errIncompleteOperation
		}

		return nil
	default:
		if err != nil {
			s.log.Error("traverse failure",
				zap.String("error", err.Error()),
			)

			err = errPlacementProblem
		} else if !p.allowPartialResult && !traverser.finished() {
			err = errIncompleteOperation
		}

		return err
	}
}

func (s *coreInterceptorPreparer) prepareInterceptor(p interceptorItems) (func(context.Context, multiaddr.Multiaddr) bool, error) {
	selfAddr, err := s.addressStore.SelfAddr()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, node multiaddr.Multiaddr) (res bool) {
		if node.Equal(selfAddr) {
			p.handler.HandleResult(ctx, selfAddr, nil,
				s.localExec.executeOperation(ctx, p.metaInfo, p.itemHandler))
			return !p.selfForward
		}

		return false
	}, nil
}

func (s *coreHandler) HandleResult(ctx context.Context, n multiaddr.Multiaddr, r interface{}, e error) {
	ok := e == nil

	s.traverser.add(n, ok)

	if ok && r != nil {
		s.itemHandler.handleItem(r)
	}

	s.resLogger.logErr(s.reqType, n, e)
}

func (s *coreResultLogger) logErr(t object.RequestType, n multiaddr.Multiaddr, e error) {
	if e == nil {
		return
	} else if _, ok := s.mLog[t]; !ok {
		return
	}

	s.log.Error("object request failure",
		zap.Stringer("type", t),
		zap.Stringer("node", n),
		zap.String("error", e.Error()),
	)
}

func (s *localOperationExecutor) executeOperation(ctx context.Context, req transport.MetaInfo, h responseItemHandler) error {
	switch req.Type() {
	case object.RequestPut:
		obj := req.(transport.PutInfo).GetHead()
		if err := s.objStore.putObject(ctx, obj); err != nil {
			return err
		}

		h.handleItem(obj.Address())
	case object.RequestGet:
		obj, err := s.objRecv.getObject(ctx, req.(transport.AddressInfo).GetAddress())
		if err != nil {
			return err
		}

		h.handleItem(obj)
	case object.RequestHead:
		head, err := s.headRecv.headObject(ctx, req.(transport.AddressInfo).GetAddress())
		if err != nil {
			return err
		}

		h.handleItem(head)
	case object.RequestSearch:
		r := req.(transport.SearchInfo)

		addrList, err := s.queryImp.imposeQuery(ctx, r.GetCID(), r.GetQuery(), 1) // TODO: add query version to SearchInfo
		if err != nil {
			return err
		}

		h.handleItem(addrList)
	case object.RequestRange:
		r := req.(transport.RangeInfo)

		rangesData, err := s.rngReader.getRange(ctx, r.GetAddress(), r.GetRange())
		if err != nil {
			return err
		}

		h.handleItem(bytes.NewReader(rangesData))
	case object.RequestRangeHash:
		r := req.(transport.RangeHashInfo)

		rangesHashes, err := s.rngHasher.getHashes(ctx, r.GetAddress(), r.GetRanges(), r.GetSalt())
		if err != nil {
			return err
		}

		h.handleItem(rangesHashes)
	default:
		return errors.Errorf(pmWrongRequestType, req)
	}

	return nil
}

func (s *localStoreExecutor) getHashes(ctx context.Context, addr Address, ranges []Range, salt []byte) ([]Hash, error) {
	res := make([]Hash, 0, len(ranges))

	for i := range ranges {
		chunk, err := s.localStore.PRead(ctx, addr, ranges[i])
		if err != nil {
			return nil, errors.Wrapf(err, emRangeReadFail, i+1)
		}

		res = append(res, hash.Sum(s.salitor(chunk, salt)))
	}

	return res, nil
}

func (s *localStoreExecutor) getRange(ctx context.Context, addr Address, r Range) ([]byte, error) {
	return s.localStore.PRead(ctx, addr, r)
}

func (s *localStoreExecutor) putObject(ctx context.Context, obj *Object) error {
	ctx = context.WithValue(ctx, localstore.StoreEpochValue, s.epochRecv.Epoch())

	switch err := s.localStore.Put(ctx, obj); err {
	// TODO: add all error cases
	case nil:
		return nil
	default:
		return errPutLocal
	}
}

func (s *localStoreExecutor) headObject(_ context.Context, addr Address) (*Object, error) {
	m, err := s.localStore.Meta(addr)
	if err != nil {
		switch errors.Cause(err) {
		case core.ErrNotFound:
			return nil, errIncompleteOperation
		default:
			return nil, err
		}
	}

	return m.Object, nil
}

func (s *localStoreExecutor) getObject(_ context.Context, addr Address) (*Object, error) {
	obj, err := s.localStore.Get(addr)
	if err != nil {
		switch errors.Cause(err) {
		case core.ErrNotFound:
			return nil, errIncompleteOperation
		default:
			return nil, err
		}
	}

	return obj, nil
}
