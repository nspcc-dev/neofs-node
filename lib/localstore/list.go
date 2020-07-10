package localstore

import (
	"context"

	"go.uber.org/zap"
)

func (l *localstore) Iterate(filter FilterPipeline, handler MetaHandler) error {
	if handler == nil {
		return ErrEmptyMetaHandler
	} else if filter == nil {
		filter = NewFilter(&FilterParams{
			Name:       "SKIPPING_FILTER",
			FilterFunc: SkippingFilterFunc,
		})
	}

	return l.metaBucket.Iterate(func(_, v []byte) bool {
		meta := new(ObjectMeta)
		if err := meta.Unmarshal(v); err != nil {
			l.log.Error("unmarshal meta bucket item failure", zap.Error(err))
		} else if filter.Pass(context.TODO(), meta).Code() == CodePass {
			return !handler(meta)
		}
		return true
	})
}

// ListItems iterates over Iterator with FilterPipeline and returns all passed items.
func ListItems(it Iterator, f FilterPipeline) ([]ListItem, error) {
	res := make([]ListItem, 0)
	err := it.Iterate(f, func(meta *ObjectMeta) (stop bool) {
		res = append(res, ListItem{
			ObjectMeta: *meta,
		})
		return
	})

	return res, err
}
