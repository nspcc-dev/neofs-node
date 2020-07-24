package transport

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

/*
	File source code includes implementation of unified objects container handler.
	Implementation provides the opportunity to perform any logic over object container distributed in network.
	Implementation holds placement and object transport implementations in a black box.
	Any special logic could be tuned through passing handle parameters.
	NOTE: Although the implementation of the other interfaces via OCH is the same, they are still separated in order to avoid mess.
*/

type (
	// SelectiveContainerExecutor is an interface the tool that performs
	// object operations in container with preconditions.
	SelectiveContainerExecutor interface {
		Put(context.Context, *PutParams) error
		Get(context.Context, *GetParams) error
		Head(context.Context, *HeadParams) error
		Search(context.Context, *SearchParams) error
		RangeHash(context.Context, *RangeHashParams) error
	}

	// PutParams groups the parameters
	// of selective object Put.
	PutParams struct {
		SelectiveParams
		Object  *object.Object
		Handler func(multiaddr.Multiaddr, bool)

		CopiesNumber uint32
	}

	// GetParams groups the parameters
	// of selective object Get.
	GetParams struct {
		SelectiveParams
		Handler func(multiaddr.Multiaddr, *object.Object)
	}

	// HeadParams groups the parameters
	// of selective object Head.
	HeadParams struct {
		GetParams
		FullHeaders bool
	}

	// SearchParams groups the parameters
	// of selective object Search.
	SearchParams struct {
		SelectiveParams
		SearchCID   refs.CID
		SearchQuery []byte
		Handler     func(multiaddr.Multiaddr, []refs.Address)
	}

	// RangeHashParams groups the parameters
	// of selective object GetRangeHash.
	RangeHashParams struct {
		SelectiveParams
		Ranges  []object.Range
		Salt    []byte
		Handler func(multiaddr.Multiaddr, []hash.Hash)
	}

	// SelectiveParams groups the parameters of
	// the execution of selective container operation.
	SelectiveParams struct {
		/* Should be set to true only if service under object transport implementations is served on localhost. */
		ServeLocal bool

		/* Raw option of the request */
		Raw bool

		/* TTL for object transport. All transport operations inherit same value. */
		TTL uint32

		/* Required ID of processing container. If empty or not set, an error is returned. */
		CID container.ID

		/* List of nodes selected for processing. If not specified => nodes will be selected during. */
		Nodes []multiaddr.Multiaddr

		/*
			Next two parameters provide the opportunity to process selective objects in container.
			At least on of non-empty IDList or Query is required, an error is returned otherwise.
		*/

		/* List of objects to process (overlaps query). */
		IDList []refs.ObjectID
		/* If no objects is indicated, query is used for selection. */
		Query []byte

		/*
			If function provided, it is called after every successful operation.
			True result breaks operation performing.
		*/
		Breaker func(refs.Address) ProgressControlFlag

		/* Public session token */
		Token service.SessionToken

		/* Bearer token */
		Bearer service.BearerToken

		/* Extended headers */
		ExtendedHeaders []service.ExtendedHeader
	}

	// ProgressControlFlag is an enumeration of progress control flags.
	ProgressControlFlag int

	// ObjectContainerHandlerParams grops the parameters of SelectiveContainerExecutor constructor.
	ObjectContainerHandlerParams struct {
		NodeLister *placement.PlacementWrapper
		Executor   ContainerTraverseExecutor
		*zap.Logger
	}

	simpleTraverser struct {
		*sync.Once
		list []multiaddr.Multiaddr
	}

	selectiveCnrExec struct {
		cnl      *placement.PlacementWrapper
		Executor ContainerTraverseExecutor
		log      *zap.Logger
	}

	metaInfo struct {
		ttl uint32
		raw bool
		rt  object.RequestType

		token service.SessionToken

		bearer service.BearerToken

		extHdrs []service.ExtendedHeader
	}

	putInfo struct {
		metaInfo
		obj *object.Object
		cn  uint32
	}

	getInfo struct {
		metaInfo
		addr object.Address
		raw  bool
	}

	headInfo struct {
		getInfo
		fullHdr bool
	}

	searchInfo struct {
		metaInfo
		cid   container.ID
		query []byte
	}

	rangeHashInfo struct {
		metaInfo
		addr   object.Address
		ranges []object.Range
		salt   []byte
	}

	execItems struct {
		params          SelectiveParams
		metaConstructor func(addr object.Address) MetaInfo
		handler         ResultHandler
	}

	searchTarget struct {
		list []refs.Address
	}

	// ContainerTraverseExecutor is an interface of
	// object operation executor with container traversing.
	ContainerTraverseExecutor interface {
		Execute(context.Context, TraverseParams)
	}

	// TraverseParams groups the parameters of container traversing.
	TraverseParams struct {
		TransportInfo        MetaInfo
		Handler              ResultHandler
		Traverser            Traverser
		WorkerPool           WorkerPool
		ExecutionInterceptor func(context.Context, multiaddr.Multiaddr) bool
	}

	// WorkerPool is an interface of go-routine pool
	WorkerPool interface {
		Submit(func()) error
	}

	// Traverser is an interface of container traverser.
	Traverser interface {
		Next(context.Context) []multiaddr.Multiaddr
	}

	cnrTraverseExec struct {
		transport ObjectTransport
	}

	singleRoutinePool struct{}

	emptyReader struct{}
)

const (
	_ ProgressControlFlag = iota

	// NextAddress is a ProgressControlFlag of to go to the next address of the object.
	NextAddress

	// NextNode is a ProgressControlFlag of to go to the next node.
	NextNode

	// BreakProgress is a ProgressControlFlag to interrupt the execution.
	BreakProgress
)

const (
	instanceFailMsg = "could not create  container objects collector"
)

var (
	errNilObjectTransport    = errors.New("object transport is nil")
	errEmptyLogger           = errors.New("empty logger")
	errEmptyNodeLister       = errors.New("empty container node lister")
	errEmptyTraverseExecutor = errors.New("empty container traverse executor")
	errSelectiveParams       = errors.New("neither ID list nor query provided")
)

func (s *selectiveCnrExec) Put(ctx context.Context, p *PutParams) error {
	meta := &putInfo{
		metaInfo: metaInfo{
			ttl: p.TTL,
			rt:  object.RequestPut,
			raw: p.Raw,

			token: p.Token,

			bearer: p.Bearer,

			extHdrs: p.ExtendedHeaders,
		},
		obj: p.Object,
		cn:  p.CopiesNumber,
	}

	return s.exec(ctx, &execItems{
		params:          p.SelectiveParams,
		metaConstructor: func(object.Address) MetaInfo { return meta },
		handler:         p,
	})
}

func (s *selectiveCnrExec) Get(ctx context.Context, p *GetParams) error {
	return s.exec(ctx, &execItems{
		params: p.SelectiveParams,
		metaConstructor: func(addr object.Address) MetaInfo {
			return &getInfo{
				metaInfo: metaInfo{
					ttl: p.TTL,
					rt:  object.RequestGet,
					raw: p.Raw,

					token: p.Token,

					bearer: p.Bearer,

					extHdrs: p.ExtendedHeaders,
				},
				addr: addr,
				raw:  p.Raw,
			}
		},
		handler: p,
	})
}

func (s *selectiveCnrExec) Head(ctx context.Context, p *HeadParams) error {
	return s.exec(ctx, &execItems{
		params: p.SelectiveParams,
		metaConstructor: func(addr object.Address) MetaInfo {
			return &headInfo{
				getInfo: getInfo{
					metaInfo: metaInfo{
						ttl: p.TTL,
						rt:  object.RequestHead,
						raw: p.Raw,

						token: p.Token,

						bearer: p.Bearer,

						extHdrs: p.ExtendedHeaders,
					},
					addr: addr,
					raw:  p.Raw,
				},
				fullHdr: p.FullHeaders,
			}
		},
		handler: p,
	})
}

func (s *selectiveCnrExec) Search(ctx context.Context, p *SearchParams) error {
	return s.exec(ctx, &execItems{
		params: p.SelectiveParams,
		metaConstructor: func(object.Address) MetaInfo {
			return &searchInfo{
				metaInfo: metaInfo{
					ttl: p.TTL,
					rt:  object.RequestSearch,
					raw: p.Raw,

					token: p.Token,

					bearer: p.Bearer,

					extHdrs: p.ExtendedHeaders,
				},
				cid:   p.SearchCID,
				query: p.SearchQuery,
			}
		},
		handler: p,
	})
}

func (s *selectiveCnrExec) RangeHash(ctx context.Context, p *RangeHashParams) error {
	return s.exec(ctx, &execItems{
		params: p.SelectiveParams,
		metaConstructor: func(addr object.Address) MetaInfo {
			return &rangeHashInfo{
				metaInfo: metaInfo{
					ttl: p.TTL,
					rt:  object.RequestRangeHash,
					raw: p.Raw,

					token: p.Token,

					bearer: p.Bearer,

					extHdrs: p.ExtendedHeaders,
				},
				addr:   addr,
				ranges: p.Ranges,
				salt:   p.Salt,
			}
		},
		handler: p,
	})
}

func (s *selectiveCnrExec) exec(ctx context.Context, p *execItems) error {
	if err := p.params.validate(); err != nil {
		return err
	}

	nodes, err := s.prepareNodes(ctx, &p.params)
	if err != nil {
		return err
	}

loop:
	for i := range nodes {
		addrList := s.prepareAddrList(ctx, &p.params, nodes[i])
		if len(addrList) == 0 {
			continue
		}

		for j := range addrList {
			if p.params.Breaker != nil {
				switch cFlag := p.params.Breaker(addrList[j]); cFlag {
				case NextAddress:
					continue
				case NextNode:
					continue loop
				case BreakProgress:
					break loop
				}
			}

			s.Executor.Execute(ctx, TraverseParams{
				TransportInfo: p.metaConstructor(addrList[j]),
				Handler:       p.handler,
				Traverser:     newSimpleTraverser(nodes[i]),
			})
		}
	}

	return nil
}

func (s *SelectiveParams) validate() error {
	switch {
	case len(s.IDList) == 0 && len(s.Query) == 0:
		return errSelectiveParams
	default:
		return nil
	}
}

func (s *selectiveCnrExec) prepareNodes(ctx context.Context, p *SelectiveParams) ([]multiaddr.Multiaddr, error) {
	if len(p.Nodes) > 0 {
		return p.Nodes, nil
	}

	// If node serves Object transport service on localhost => pass single empty node
	if p.ServeLocal {
		// all transport implementations will use localhost by default
		return []multiaddr.Multiaddr{nil}, nil
	}

	// Otherwise use container nodes
	return s.cnl.ContainerNodes(ctx, p.CID)
}

func (s *selectiveCnrExec) prepareAddrList(ctx context.Context, p *SelectiveParams, node multiaddr.Multiaddr) []refs.Address {
	var (
		addrList []object.Address
		l        = len(p.IDList)
	)

	if l > 0 {
		addrList = make([]object.Address, 0, l)
		for i := range p.IDList {
			addrList = append(addrList, object.Address{CID: p.CID, ObjectID: p.IDList[i]})
		}

		return addrList
	}

	handler := new(searchTarget)

	s.Executor.Execute(ctx, TraverseParams{
		TransportInfo: &searchInfo{
			metaInfo: metaInfo{
				ttl: p.TTL,
				rt:  object.RequestSearch,
				raw: p.Raw,

				token: p.Token,

				bearer: p.Bearer,

				extHdrs: p.ExtendedHeaders,
			},
			cid:   p.CID,
			query: p.Query,
		},
		Handler:   handler,
		Traverser: newSimpleTraverser(node),
	})

	return handler.list
}

func newSimpleTraverser(list ...multiaddr.Multiaddr) Traverser {
	return &simpleTraverser{
		Once: new(sync.Once),
		list: list,
	}
}

func (s *simpleTraverser) Next(context.Context) (res []multiaddr.Multiaddr) {
	s.Do(func() {
		res = s.list
	})

	return
}

func (s metaInfo) GetTTL() uint32 { return s.ttl }

func (s metaInfo) GetTimeout() time.Duration { return 0 }

func (s metaInfo) GetRaw() bool { return s.raw }

func (s metaInfo) Type() object.RequestType { return s.rt }

func (s metaInfo) GetSessionToken() service.SessionToken { return s.token }

func (s metaInfo) GetBearerToken() service.BearerToken { return s.bearer }

func (s metaInfo) ExtendedHeaders() []service.ExtendedHeader { return s.extHdrs }

func (s *putInfo) GetHead() *object.Object { return s.obj }

func (s *putInfo) Payload() io.Reader { return new(emptyReader) }

func (*emptyReader) Read(p []byte) (int, error) { return 0, io.EOF }

func (s *putInfo) CopiesNumber() uint32 {
	return s.cn
}

func (s *getInfo) GetAddress() refs.Address { return s.addr }

func (s *getInfo) Raw() bool { return s.raw }

func (s *headInfo) GetFullHeaders() bool { return s.fullHdr }

func (s *searchInfo) GetCID() refs.CID { return s.cid }

func (s *searchInfo) GetQuery() []byte { return s.query }

func (s *rangeHashInfo) GetAddress() refs.Address { return s.addr }

func (s *rangeHashInfo) GetRanges() []object.Range { return s.ranges }

func (s *rangeHashInfo) GetSalt() []byte { return s.salt }

func (s *searchTarget) HandleResult(_ context.Context, _ multiaddr.Multiaddr, r interface{}, e error) {
	if e == nil {
		s.list = append(s.list, r.([]refs.Address)...)
	}
}

// HandleResult calls Handler with:
//  - Multiaddr with argument value;
//  - error equality to nil.
func (s *PutParams) HandleResult(_ context.Context, node multiaddr.Multiaddr, _ interface{}, e error) {
	s.Handler(node, e == nil)
}

// HandleResult calls Handler if error argument is nil with:
//  - Multiaddr with argument value;
//  - result casted to an Object pointer.
func (s *GetParams) HandleResult(_ context.Context, node multiaddr.Multiaddr, r interface{}, e error) {
	if e == nil {
		s.Handler(node, r.(*object.Object))
	}
}

// HandleResult calls Handler if error argument is nil with:
//  - Multiaddr with argument value;
//  - result casted to Address slice.
func (s *SearchParams) HandleResult(_ context.Context, node multiaddr.Multiaddr, r interface{}, e error) {
	if e == nil {
		s.Handler(node, r.([]refs.Address))
	}
}

// HandleResult calls Handler if error argument is nil with:
//  - Multiaddr with argument value;
//  - result casted to Hash slice.
func (s *RangeHashParams) HandleResult(_ context.Context, node multiaddr.Multiaddr, r interface{}, e error) {
	if e == nil {
		s.Handler(node, r.([]hash.Hash))
	}
}

func (s *cnrTraverseExec) Execute(ctx context.Context, p TraverseParams) {
	if p.WorkerPool == nil {
		p.WorkerPool = new(singleRoutinePool)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := new(sync.WaitGroup)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		nodes := p.Traverser.Next(ctx)
		if len(nodes) == 0 {
			break
		}

		for i := range nodes {
			node := nodes[i]

			wg.Add(1)

			if err := p.WorkerPool.Submit(func() {
				defer wg.Done()

				if p.ExecutionInterceptor != nil && p.ExecutionInterceptor(ctx, node) {
					return
				}

				s.transport.Transport(ctx, ObjectTransportParams{
					TransportInfo: p.TransportInfo,
					TargetNode:    node,
					ResultHandler: p.Handler,
				})
			}); err != nil {
				wg.Done()
			}
		}

		wg.Wait()
	}
}

func (*singleRoutinePool) Submit(fn func()) error {
	fn()
	return nil
}

// NewObjectContainerHandler is a SelectiveContainerExecutor constructor.
func NewObjectContainerHandler(p ObjectContainerHandlerParams) (SelectiveContainerExecutor, error) {
	switch {
	case p.Executor == nil:
		return nil, errors.Wrap(errEmptyTraverseExecutor, instanceFailMsg)
	case p.Logger == nil:
		return nil, errors.Wrap(errEmptyLogger, instanceFailMsg)
	case p.NodeLister == nil:
		return nil, errors.Wrap(errEmptyNodeLister, instanceFailMsg)
	}

	return &selectiveCnrExec{
		cnl:      p.NodeLister,
		Executor: p.Executor,
		log:      p.Logger,
	}, nil
}

// NewContainerTraverseExecutor is a ContainerTraverseExecutor executor.
func NewContainerTraverseExecutor(t ObjectTransport) (ContainerTraverseExecutor, error) {
	if t == nil {
		return nil, errNilObjectTransport
	}

	return &cnrTraverseExec{transport: t}, nil
}
