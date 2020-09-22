package searchsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
	"github.com/pkg/errors"
)

type localStream struct {
	query query.Query

	storage *localstore.Storage
}

func (s *localStream) stream(ctx context.Context, ch chan<- []*object.ID) error {
	idList := make([]*object.ID, 0)

	if err := s.storage.Iterate(newFilterPipeline(s.query), func(meta *localstore.ObjectMeta) bool {
		select {
		case <-ctx.Done():
			return true
		default:
			idList = append(idList, meta.Head().GetID())

			return false
		}
	}); err != nil && !errors.Is(errors.Cause(err), bucket.ErrIteratingAborted) {
		return errors.Wrapf(err, "(%T) could not iterate over local storage", s)
	}

	ch <- idList

	return nil
}

func newFilterPipeline(q query.Query) localstore.FilterPipeline {
	res := localstore.NewFilter(&localstore.FilterParams{
		Name: "SEARCH_OBJECTS_FILTER",
		FilterFunc: func(context.Context, *localstore.ObjectMeta) *localstore.FilterResult {
			return localstore.ResultPass()
		},
	})

	if err := res.PutSubFilter(localstore.SubFilterParams{
		FilterPipeline: localstore.NewFilter(&localstore.FilterParams{
			FilterFunc: func(_ context.Context, o *localstore.ObjectMeta) *localstore.FilterResult {
				if !q.Match(o.Head()) {
					return localstore.ResultFail()
				}
				return localstore.ResultPass()
			},
		}),
		OnIgnore: localstore.CodeFail,
		OnFail:   localstore.CodeFail,
	}); err != nil {
		panic(errors.Wrap(err, "could not create all pass including filter"))
	}

	return res
}
