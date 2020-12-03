package shard

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.uber.org/zap"
)

func (s *Shard) List() (*SelectRes, error) {
	lst, err := s.metaBase.Containers()
	if err != nil {
		return nil, fmt.Errorf("can't list stored containers: %w", err)
	}

	res := new(SelectRes)
	filters := object.NewSearchFilters()

	for i := range lst {
		filters = filters[:0]
		filters.AddObjectContainerIDFilter(object.MatchStringEqual, lst[i])

		ids, err := s.metaBase.Select(filters) // consider making List in metabase
		if err != nil {
			s.log.Debug("can't select all objects",
				zap.Stringer("cid", lst[i]),
				zap.String("error", err.Error()))

			continue
		}

		res.addrList = append(res.addrList, ids...)
	}

	return res, nil
}
