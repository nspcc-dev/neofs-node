package transformer

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/pkg/errors"
)

type (
	// ObjectRestorer is an interface of object restorer.
	ObjectRestorer interface {
		Type() object.Transform_Type
		Restore(context.Context, ...Object) ([]Object, error)
	}

	restorePipeline struct {
		ObjectRestorer
		*sync.RWMutex
		items map[object.Transform_Type]ObjectRestorer
	}

	splitRestorer struct{}
)

var errEmptyObjList = errors.New("object list is empty")

var errMissingParentLink = errors.New("missing parent link")

func (s *restorePipeline) Restore(ctx context.Context, srcObjs ...Object) ([]Object, error) {
	if len(srcObjs) == 0 {
		return nil, errEmptyInput
	}

	s.RLock()
	defer s.RUnlock()

	var (
		objs = srcObjs
		err  error
	)

	for {
		_, th := objs[0].LastHeader(object.HeaderType(object.TransformHdr))
		if th == nil {
			break
		}

		transform := th.Value.(*object.Header_Transform).Transform

		tr, ok := s.items[transform.Type]
		if !ok {
			return nil, errors.Errorf("missing restorer (%s)", transform.Type)
		}

		if objs, err = tr.Restore(ctx, objs...); err != nil {
			return nil, errors.Wrapf(err, "restoration failed (%s)", transform.Type)
		}
	}

	return objs, nil
}

// NewRestorePipeline is a constructor of the pipeline of object restorers.
func NewRestorePipeline(t ...ObjectRestorer) ObjectRestorer {
	m := make(map[object.Transform_Type]ObjectRestorer, len(t))

	for i := range t {
		m[t[i].Type()] = t[i]
	}

	return &restorePipeline{
		RWMutex: new(sync.RWMutex),
		items:   m,
	}
}

func (*splitRestorer) Type() object.Transform_Type {
	return object.Transform_Split
}

func (*splitRestorer) Restore(ctx context.Context, objs ...Object) ([]Object, error) {
	if len(objs) == 0 {
		return nil, errEmptyObjList
	}

	chain, err := GetChain(objs...)
	if err != nil {
		return nil, errors.Wrap(err, "could not get chain of objects")
	}

	obj := chain[len(chain)-1]

	var (
		size uint64
		p    = make([]byte, 0, len(chain[0].Payload)*len(chain))
	)

	for j := 0; j < len(chain); j++ {
		p = append(p, chain[j].Payload...)
		size += chain[j].SystemHeader.PayloadLength
	}

	obj.SystemHeader.PayloadLength = size
	obj.Payload = p

	parent, err := lastLink(&obj, object.Link_Parent)
	if err != nil {
		return nil, errMissingParentLink
	}

	obj.SystemHeader.ID = parent

	err = deleteTransformer(&obj, object.Transform_Split)
	if err != nil {
		return nil, err
	}

	return []Object{obj}, nil
}

// SplitRestorer is a splitted object restorer's constructor.
func SplitRestorer() ObjectRestorer {
	return new(splitRestorer)
}
