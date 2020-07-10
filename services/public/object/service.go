package object

import (
	"context"
	"crypto/ecdsa"
	"math"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-api-go/storagegroup"
	"github.com/nspcc-dev/neofs-node/internal"
	libacl "github.com/nspcc-dev/neofs-node/lib/acl"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/ir"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/objio"
	"github.com/nspcc-dev/neofs-node/lib/objutil"
	"github.com/nspcc-dev/neofs-node/lib/transformer"
	"github.com/nspcc-dev/neofs-node/modules/grpc"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// CID is a type alias of
	// CID from refs package of neofs-api-go.
	CID = refs.CID

	// Object is a type alias of
	// Object from object package of neofs-api-go.
	Object = object.Object

	// ID is a type alias of
	// ObjectID from refs package of neofs-api-go.
	ID = refs.ObjectID

	// OwnerID is a type alias of
	// OwnerID from refs package of neofs-api-go.
	OwnerID = refs.OwnerID

	// Address is a type alias of
	// Address from refs package of neofs-api-go.
	Address = refs.Address

	// Hash is a type alias of
	// Hash from hash package of neofs-api-go.
	Hash = hash.Hash

	// Meta is a type alias of
	// ObjectMeta from localstore package.
	Meta = localstore.ObjectMeta

	// Filter is a type alias of
	// FilterPipeline from localstore package.
	Filter = localstore.FilterPipeline

	// Header is a type alias of
	// Header from object package of neofs-api-go.
	Header = object.Header

	// UserHeader is a type alias of
	// UserHeader from object package of neofs-api-go.
	UserHeader = object.UserHeader

	// SystemHeader is a type alias of
	// SystemHeader from object package of neofs-api-go.
	SystemHeader = object.SystemHeader

	// CreationPoint is a type alias of
	// CreationPoint from object package of neofs-api-go.
	CreationPoint = object.CreationPoint

	// Service is an interface of the server of Object service.
	Service interface {
		grpc.Service
		CapacityMeter
		object.ServiceServer
	}

	// CapacityMeter is an interface of node storage capacity meter.
	CapacityMeter interface {
		RelativeAvailableCap() float64
		AbsoluteAvailableCap() uint64
	}

	// EpochReceiver is an interface of the container of epoch number with read access.
	EpochReceiver interface {
		Epoch() uint64
	}

	// RemoteService is an interface of Object service client constructor.
	RemoteService interface {
		Remote(context.Context, multiaddr.Multiaddr) (object.ServiceClient, error)
	}

	// Placer is an interface of placement component.
	Placer interface {
		IsContainerNode(ctx context.Context, addr multiaddr.Multiaddr, cid CID, previousNetMap bool) (bool, error)
		GetNodes(ctx context.Context, addr Address, usePreviousNetMap bool, excl ...multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error)
	}

	// WorkerPool is an interface of go-routing pool.
	WorkerPool interface {
		Submit(func()) error
	}

	// Salitor is a salting slice function.
	Salitor func(data []byte, salt []byte) []byte

	serviceRequest interface {
		object.Request
		service.RequestData
		service.SignKeyPairAccumulator
		service.SignKeyPairSource

		SetToken(*service.Token)

		SetBearer(*service.BearerTokenMsg)

		SetHeaders([]service.RequestExtendedHeader_KV)
	}

	// Params groups the parameters of Object service server's constructor.
	Params struct {
		CheckACL bool

		Assembly bool

		WindowSize int

		MaxProcessingSize uint64
		StorageCapacity   uint64
		PoolSize          int
		Salitor           Salitor
		LocalStore        localstore.Localstore
		Placer            Placer
		ObjectRestorer    transformer.ObjectRestorer
		RemoteService     RemoteService
		AddressStore      implementations.AddressStoreComponent
		Logger            *zap.Logger
		TokenStore        session.PrivateTokenStore
		EpochReceiver     EpochReceiver

		implementations.ContainerNodesLister

		DialTimeout time.Duration

		Key *ecdsa.PrivateKey

		PutParams       OperationParams
		GetParams       OperationParams
		DeleteParams    OperationParams
		HeadParams      OperationParams
		SearchParams    OperationParams
		RangeParams     OperationParams
		RangeHashParams OperationParams

		headRecv objectReceiver

		Verifier objutil.Verifier

		Transformer transformer.Transformer

		MaxPayloadSize uint64

		// ACL pre-processor params
		ACLHelper       implementations.ACLHelper
		BasicACLChecker libacl.BasicChecker
		IRStorage       ir.Storage
		ContainerLister implementations.ContainerNodesLister

		SGInfoReceiver storagegroup.InfoReceiver

		OwnerKeyVerifier core.OwnerKeyVerifier

		ExtendedACLSource libacl.ExtendedACLSource

		requestActionCalculator

		targetFinder RequestTargeter

		aclInfoReceiver aclInfoReceiver
	}

	// OperationParams groups the parameters of particular object operation.
	OperationParams struct {
		Timeout   time.Duration
		LogErrors bool
	}

	objectService struct {
		ls         localstore.Localstore
		storageCap uint64

		executor implementations.SelectiveContainerExecutor

		pPut     OperationParams
		pGet     OperationParams
		pDel     OperationParams
		pHead    OperationParams
		pSrch    OperationParams
		pRng     OperationParams
		pRngHash OperationParams

		log *zap.Logger

		requestHandler requestHandler

		objSearcher objectSearcher
		objRecv     objectReceiver
		objStorer   objectStorer
		objRemover  objectRemover
		rngRecv     objectRangeReceiver

		payloadRngRecv payloadRangeReceiver

		respPreparer responsePreparer

		getChunkPreparer   responsePreparer
		rangeChunkPreparer responsePreparer

		statusCalculator *statusCalculator
	}
)

const (
	defaultDialTimeout      = 5 * time.Second
	defaultPutTimeout       = time.Second
	defaultGetTimeout       = time.Second
	defaultDeleteTimeout    = time.Second
	defaultHeadTimeout      = time.Second
	defaultSearchTimeout    = time.Second
	defaultRangeTimeout     = time.Second
	defaultRangeHashTimeout = time.Second

	defaultPoolSize = 10

	readyObjectsCheckpointFilterName = "READY_OBJECTS_PUT_CHECKPOINT"
	allObjectsCheckpointFilterName   = "ALL_OBJECTS_PUT_CHECKPOINT"

	errEmptyTokenStore      = internal.Error("objectService.New failed: key store not provided")
	errEmptyPlacer          = internal.Error("objectService.New failed: placer not provided")
	errEmptyTransformer     = internal.Error("objectService.New failed: transformer pipeline not provided")
	errEmptyGRPC            = internal.Error("objectService.New failed: gRPC connector not provided")
	errEmptyAddress         = internal.Error("objectService.New failed: address store not provided")
	errEmptyLogger          = internal.Error("objectService.New failed: logger not provided")
	errEmptyEpochReceiver   = internal.Error("objectService.New failed: epoch receiver not provided")
	errEmptyLocalStore      = internal.Error("new local client failed: <nil> localstore passed")
	errEmptyPrivateKey      = internal.Error("objectService.New failed: private key not provided")
	errEmptyVerifier        = internal.Error("objectService.New failed: object verifier not provided")
	errEmptyACLHelper       = internal.Error("objectService.New failed: ACL helper not provided")
	errEmptyBasicACLChecker = internal.Error("objectService.New failed: basic ACL checker not provided")
	errEmptyCnrLister       = internal.Error("objectService.New failed: container lister not provided")
	errEmptySGInfoRecv      = internal.Error("objectService.New failed: SG info receiver not provided")

	errInvalidCIDFilter = internal.Error("invalid CID filter")

	errTokenRetrieval = internal.Error("objectService.Put failed on token retrieval")

	errHeaderExpected = internal.Error("expected header as a first message in stream")
)

var requestSignFunc = service.SignRequestData

var requestVerifyFunc = core.VerifyRequestWithSignatures

// New is an Object service server's constructor.
func New(p *Params) (Service, error) {
	if p.PutParams.Timeout <= 0 {
		p.PutParams.Timeout = defaultPutTimeout
	}

	if p.GetParams.Timeout <= 0 {
		p.GetParams.Timeout = defaultGetTimeout
	}

	if p.DeleteParams.Timeout <= 0 {
		p.DeleteParams.Timeout = defaultDeleteTimeout
	}

	if p.HeadParams.Timeout <= 0 {
		p.HeadParams.Timeout = defaultHeadTimeout
	}

	if p.SearchParams.Timeout <= 0 {
		p.SearchParams.Timeout = defaultSearchTimeout
	}

	if p.RangeParams.Timeout <= 0 {
		p.RangeParams.Timeout = defaultRangeTimeout
	}

	if p.RangeHashParams.Timeout <= 0 {
		p.RangeHashParams.Timeout = defaultRangeHashTimeout
	}

	if p.DialTimeout <= 0 {
		p.DialTimeout = defaultDialTimeout
	}

	if p.PoolSize <= 0 {
		p.PoolSize = defaultPoolSize
	}

	switch {
	case p.TokenStore == nil:
		return nil, errEmptyTokenStore
	case p.Placer == nil:
		return nil, errEmptyPlacer
	case p.LocalStore == nil:
		return nil, errEmptyLocalStore
	case (p.ObjectRestorer == nil || p.Transformer == nil) && p.Assembly:
		return nil, errEmptyTransformer
	case p.RemoteService == nil:
		return nil, errEmptyGRPC
	case p.AddressStore == nil:
		return nil, errEmptyAddress
	case p.Logger == nil:
		return nil, errEmptyLogger
	case p.EpochReceiver == nil:
		return nil, errEmptyEpochReceiver
	case p.Key == nil:
		return nil, errEmptyPrivateKey
	case p.Verifier == nil:
		return nil, errEmptyVerifier
	case p.IRStorage == nil:
		return nil, ir.ErrNilStorage
	case p.ContainerLister == nil:
		return nil, errEmptyCnrLister
	case p.ACLHelper == nil:
		return nil, errEmptyACLHelper
	case p.BasicACLChecker == nil:
		return nil, errEmptyBasicACLChecker
	case p.SGInfoReceiver == nil:
		return nil, errEmptySGInfoRecv
	case p.OwnerKeyVerifier == nil:
		return nil, core.ErrNilOwnerKeyVerifier
	case p.ExtendedACLSource == nil:
		return nil, libacl.ErrNilBinaryExtendedACLStore
	}

	pool, err := ants.NewPool(p.PoolSize)
	if err != nil {
		return nil, errors.Wrap(err, "objectService.New failed: could not create worker pool")
	}

	if p.MaxProcessingSize <= 0 {
		p.MaxProcessingSize = math.MaxUint64
	}

	if p.StorageCapacity <= 0 {
		p.StorageCapacity = math.MaxUint64
	}

	epochRespPreparer := &epochResponsePreparer{
		epochRecv: p.EpochReceiver,
	}

	p.targetFinder = &targetFinder{
		log:             p.Logger,
		irStorage:       p.IRStorage,
		cnrLister:       p.ContainerLister,
		cnrOwnerChecker: p.ACLHelper,
	}

	p.requestActionCalculator = &reqActionCalc{
		extACLChecker: libacl.NewExtendedACLChecker(),

		log: p.Logger,
	}

	p.aclInfoReceiver = aclInfoReceiver{
		basicACLGetter: p.ACLHelper,

		basicChecker: p.BasicACLChecker,

		targetFinder: p.targetFinder,
	}

	srv := &objectService{
		ls:         p.LocalStore,
		log:        p.Logger,
		pPut:       p.PutParams,
		pGet:       p.GetParams,
		pDel:       p.DeleteParams,
		pHead:      p.HeadParams,
		pSrch:      p.SearchParams,
		pRng:       p.RangeParams,
		pRngHash:   p.RangeHashParams,
		storageCap: p.StorageCapacity,

		requestHandler: &coreRequestHandler{
			preProc:  newPreProcessor(p),
			postProc: newPostProcessor(),
		},

		respPreparer: &complexResponsePreparer{
			items: []responsePreparer{
				epochRespPreparer,
				&aclResponsePreparer{
					aclInfoReceiver: p.aclInfoReceiver,

					reqActCalc: p.requestActionCalculator,

					eaclSrc: p.ExtendedACLSource,
				},
			},
		},

		getChunkPreparer: epochRespPreparer,

		rangeChunkPreparer: epochRespPreparer,

		statusCalculator: serviceStatusCalculator(),
	}

	tr, err := NewMultiTransport(MultiTransportParams{
		AddressStore:     p.AddressStore,
		EpochReceiver:    p.EpochReceiver,
		RemoteService:    p.RemoteService,
		Logger:           p.Logger,
		Key:              p.Key,
		PutTimeout:       p.PutParams.Timeout,
		GetTimeout:       p.GetParams.Timeout,
		HeadTimeout:      p.HeadParams.Timeout,
		SearchTimeout:    p.SearchParams.Timeout,
		RangeHashTimeout: p.RangeHashParams.Timeout,
		DialTimeout:      p.DialTimeout,

		PrivateTokenStore: p.TokenStore,
	})
	if err != nil {
		return nil, err
	}

	exec, err := implementations.NewContainerTraverseExecutor(tr)
	if err != nil {
		return nil, err
	}

	srv.executor, err = implementations.NewObjectContainerHandler(implementations.ObjectContainerHandlerParams{
		NodeLister: p.ContainerNodesLister,
		Executor:   exec,
		Logger:     p.Logger,
	})
	if err != nil {
		return nil, err
	}

	local := &localStoreExecutor{
		salitor:    p.Salitor,
		epochRecv:  p.EpochReceiver,
		localStore: p.LocalStore,
	}

	qvc := &queryVersionController{
		m: make(map[int]localQueryImposer),
	}

	qvc.m[1] = &coreQueryImposer{
		fCreator: new(coreFilterCreator),
		lsLister: p.LocalStore,
		log:      p.Logger,
	}

	localExec := &localOperationExecutor{
		objRecv:   local,
		headRecv:  local,
		objStore:  local,
		queryImp:  qvc,
		rngReader: local,
		rngHasher: local,
	}

	opExec := &coreOperationExecutor{
		pre: new(coreExecParamsComp),
		fin: &coreOperationFinalizer{
			curPlacementBuilder: &corePlacementUtil{
				prevNetMap:       false,
				placementBuilder: p.Placer,
				log:              p.Logger,
			},
			prevPlacementBuilder: &corePlacementUtil{
				prevNetMap:       true,
				placementBuilder: p.Placer,
				log:              p.Logger,
			},
			interceptorPreparer: &coreInterceptorPreparer{
				localExec:    localExec,
				addressStore: p.AddressStore,
			},
			workerPool:   pool,
			traverseExec: exec,
			resLogger: &coreResultLogger{
				mLog: requestLogMap(p),
				log:  p.Logger,
			},
			log: p.Logger,
		},
		loc: localExec,
	}

	srv.objSearcher = &coreObjectSearcher{
		executor: opExec,
	}

	childLister := &coreChildrenLister{
		queryFn:     coreChildrenQueryFunc,
		objSearcher: srv.objSearcher,
		log:         p.Logger,
		timeout:     p.SearchParams.Timeout,
	}

	childrenRecv := &coreChildrenReceiver{
		timeout: p.HeadParams.Timeout,
	}

	chopperTable := objio.NewChopperTable()

	relRecv := &neighborReceiver{
		firstChildQueryFn:    firstChildQueryFunc,
		leftNeighborQueryFn:  leftNeighborQueryFunc,
		rightNeighborQueryFn: rightNeighborQueryFunc,
		rangeDescRecv:        &selectiveRangeRecv{executor: srv.executor},
	}

	straightObjRecv := &straightObjectReceiver{
		executor: opExec,
	}

	rngRecv := &corePayloadRangeReceiver{
		chopTable: chopperTable,
		relRecv:   relRecv,
		payloadRecv: &corePayloadPartReceiver{
			rDataRecv: &straightRangeDataReceiver{
				executor: opExec,
			},
			windowController: &simpleWindowController{
				windowSize: p.WindowSize,
			},
		},
		mErr: map[error]struct{}{
			localstore.ErrOutOfRange: {},
		},
		log: p.Logger,
	}

	coreObjRecv := &coreObjectReceiver{
		straightObjRecv: straightObjRecv,
		childLister:     childLister,
		ancestralRecv: &coreAncestralReceiver{
			childrenRecv: childrenRecv,
			objRewinder: &coreObjectRewinder{
				transformer: p.ObjectRestorer,
			},
			pRangeRecv: rngRecv,
		},
		log: p.Logger,
	}
	childrenRecv.coreObjRecv = coreObjRecv
	srv.objRecv = coreObjRecv
	srv.payloadRngRecv = rngRecv

	if !p.Assembly {
		coreObjRecv.ancestralRecv, coreObjRecv.childLister = nil, nil
	}

	p.headRecv = srv.objRecv

	filter, err := newIncomingObjectFilter(p)
	if err != nil {
		return nil, err
	}

	straightStorer := &straightObjectStorer{
		executor: opExec,
	}

	bf, err := basicFilter(p)
	if err != nil {
		return nil, err
	}

	transformerObjStorer := &transformingObjectStorer{
		transformer: p.Transformer,
		objStorer:   straightStorer,
		mErr: map[error]struct{}{
			transformer.ErrInvalidSGLinking: {},

			implementations.ErrIncompleteSGInfo: {},
		},
	}

	srv.objStorer = &filteringObjectStorer{
		filter: bf,
		objStorer: &bifurcatingObjectStorer{
			straightStorer: &filteringObjectStorer{
				filter: filter,
				objStorer: &receivingObjectStorer{
					straightStorer: straightStorer,
					vPayload:       implementations.NewPayloadVerifier(),
				},
			},
			tokenStorer: &tokenObjectStorer{
				tokenStore: p.TokenStore,
				objStorer:  transformerObjStorer,
			},
		},
	}

	srv.objRemover = &coreObjRemover{
		delPrep: &coreDelPreparer{
			childLister: childLister,
		},
		straightRem: &straightObjRemover{
			tombCreator: new(coreTombCreator),
			objStorer:   transformerObjStorer,
		},
		tokenStore: p.TokenStore,
		mErr:       map[error]struct{}{},
		log:        p.Logger,
	}

	srv.rngRecv = &coreRangeReceiver{
		rngRevealer: &coreRngRevealer{
			relativeRecv: relRecv,
			chopTable:    chopperTable,
		},
		straightRngRecv: &straightRangeReceiver{
			executor: opExec,
		},
		mErr: map[error]struct{}{
			localstore.ErrOutOfRange: {},
		},
		log: p.Logger,
	}

	return srv, nil
}

func requestLogMap(p *Params) map[object.RequestType]struct{} {
	m := make(map[object.RequestType]struct{})

	if p.PutParams.LogErrors {
		m[object.RequestPut] = struct{}{}
	}

	if p.GetParams.LogErrors {
		m[object.RequestGet] = struct{}{}
	}

	if p.HeadParams.LogErrors {
		m[object.RequestHead] = struct{}{}
	}

	if p.SearchParams.LogErrors {
		m[object.RequestSearch] = struct{}{}
	}

	if p.RangeParams.LogErrors {
		m[object.RequestRange] = struct{}{}
	}

	if p.RangeHashParams.LogErrors {
		m[object.RequestRangeHash] = struct{}{}
	}

	return m
}

func (s *objectService) Name() string { return "Object Service" }

func (s *objectService) Register(g *grpc.Server) { object.RegisterServiceServer(g, s) }
