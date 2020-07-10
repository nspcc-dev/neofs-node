package localstore

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/nspcc-dev/neofs-node/lib/metrics"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// Localstore is an interface of local object storage.
	Localstore interface {
		Put(context.Context, *Object) error
		Get(Address) (*Object, error)
		Del(Address) error
		Meta(Address) (*ObjectMeta, error)
		Iterator
		Has(Address) (bool, error)
		ObjectsCount() (uint64, error)

		object.PositionReader
		Size() int64
	}

	// MetaHandler is a function that handles ObjectMeta.
	MetaHandler func(*ObjectMeta) bool

	// Iterator is an interface of the iterator over local object storage.
	Iterator interface {
		Iterate(FilterPipeline, MetaHandler) error
	}

	// ListItem is an ObjectMeta wrapper.
	ListItem struct {
		ObjectMeta
	}

	// Params groups the parameters of
	// local object storage constructor.
	Params struct {
		BlobBucket core.Bucket
		MetaBucket core.Bucket
		Logger     *zap.Logger
		Collector  metrics.Collector
	}

	localstore struct {
		metaBucket core.Bucket
		blobBucket core.Bucket

		log *zap.Logger
		col metrics.Collector
	}
)

// ErrOutOfRange is returned when requested object payload range is
// out of object payload bounds.
var ErrOutOfRange = errors.New("range is out of payload bounds")

// ErrEmptyMetaHandler is returned by functions that expect
// a non-nil MetaHandler, but received nil.
var ErrEmptyMetaHandler = errors.New("meta handler is nil")

var errNilLogger = errors.New("logger is nil")

var errNilCollector = errors.New("metrics collector is nil")

// New is a local object storage constructor.
func New(p Params) (Localstore, error) {
	switch {
	case p.MetaBucket == nil:
		return nil, errors.Errorf("%s bucket is nil", core.MetaStore)
	case p.BlobBucket == nil:
		return nil, errors.Errorf("%s bucket is nil", core.BlobStore)
	case p.Logger == nil:
		return nil, errNilLogger
	case p.Collector == nil:
		return nil, errNilCollector
	}

	return &localstore{
		metaBucket: p.MetaBucket,
		blobBucket: p.BlobBucket,
		log:        p.Logger,
		col:        p.Collector,
	}, nil
}

func (l localstore) Size() int64 { return l.blobBucket.Size() }

// TODO: implement less costly method of counting.
func (l localstore) ObjectsCount() (uint64, error) {
	items, err := l.metaBucket.List()
	if err != nil {
		return 0, err
	}

	return uint64(len(items)), nil
}
