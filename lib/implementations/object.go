package implementations

import (
	"context"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/replication"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// ObjectStorage is an interface of encapsulated ObjectReceptacle and ObjectSource pair.
	ObjectStorage interface {
		replication.ObjectReceptacle
		replication.ObjectSource
	}

	objectStorage struct {
		ls       localstore.Localstore
		executor SelectiveContainerExecutor
		log      *zap.Logger
	}

	// ObjectStorageParams groups the parameters of ObjectStorage constructor.
	ObjectStorageParams struct {
		Localstore                 localstore.Localstore
		SelectiveContainerExecutor SelectiveContainerExecutor
		Logger                     *zap.Logger
	}
)

const objectSourceInstanceFailMsg = "could not create object source"

var errNilObject = errors.New("object is nil")

var errCouldNotGetObject = errors.New("could not get object from any node")

func (s *objectStorage) Put(ctx context.Context, params replication.ObjectStoreParams) error {
	if params.Object == nil {
		return errNilObject
	} else if len(params.Nodes) == 0 {
		if s.ls == nil {
			return errEmptyLocalstore
		}
		return s.ls.Put(ctx, params.Object)
	}

	nodes := make([]multiaddr.Multiaddr, len(params.Nodes))
	for i := range params.Nodes {
		nodes[i] = params.Nodes[i].Node
	}

	return s.executor.Put(ctx, &PutParams{
		SelectiveParams: SelectiveParams{
			CID:    params.Object.SystemHeader.CID,
			Nodes:  nodes,
			TTL:    service.NonForwardingTTL,
			IDList: make([]ObjectID, 1),
		},
		Object: params.Object,
		Handler: func(node multiaddr.Multiaddr, valid bool) {
			if params.Handler == nil {
				return
			}
			for i := range params.Nodes {
				if params.Nodes[i].Node.Equal(node) {
					params.Handler(params.Nodes[i], valid)
					return
				}
			}
		},
	})
}

func (s *objectStorage) Get(ctx context.Context, addr Address) (res *Object, err error) {
	if s.ls != nil {
		if has, err := s.ls.Has(addr); err == nil && has {
			if res, err = s.ls.Get(addr); err == nil {
				return res, err
			}
		}
	}

	if err = s.executor.Get(ctx, &GetParams{
		SelectiveParams: SelectiveParams{
			CID:    addr.CID,
			TTL:    service.NonForwardingTTL,
			IDList: []ObjectID{addr.ObjectID},
			Breaker: func(refs.Address) (cFlag ProgressControlFlag) {
				if res != nil {
					cFlag = BreakProgress
				}
				return
			},
		},
		Handler: func(node multiaddr.Multiaddr, obj *object.Object) { res = obj },
	}); err != nil {
		return
	} else if res == nil {
		return nil, errCouldNotGetObject
	}

	return
}

// NewObjectStorage encapsulates Localstore and SelectiveContainerExecutor
// and returns ObjectStorage interface.
func NewObjectStorage(p ObjectStorageParams) (ObjectStorage, error) {
	if p.Logger == nil {
		return nil, errors.Wrap(errEmptyLogger, objectSourceInstanceFailMsg)
	}

	if p.Localstore == nil {
		p.Logger.Warn("local storage not provided")
	}

	if p.SelectiveContainerExecutor == nil {
		p.Logger.Warn("object container handler not provided")
	}

	return &objectStorage{
		ls:       p.Localstore,
		executor: p.SelectiveContainerExecutor,
		log:      p.Logger,
	}, nil
}
