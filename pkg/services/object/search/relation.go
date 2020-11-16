package searchsvc

import (
	"context"
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
	queryV1 "github.com/nspcc-dev/neofs-node/pkg/services/object/search/query/v1"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

type RelationSearcher struct {
	svc *Service

	queryGenerator func(*object.Address) query.Query
}

var ErrRelationNotFound = errors.New("relation not found")

func (s *RelationSearcher) SearchRelation(ctx context.Context, addr *object.Address, prm *util.CommonPrm) (*object.ID, error) {
	streamer, err := s.svc.Search(ctx, new(Prm).
		WithContainerID(addr.ContainerID()).WithCommonPrm(prm).
		WithSearchQuery(s.queryGenerator(addr)),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not create search streamer", s)
	}

	res, err := readFullStream(streamer, 1)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not read full search stream", s)
	} else if ln := len(res); ln != 1 {
		if ln == 0 {
			return nil, ErrRelationNotFound
		}

		return nil, errors.Errorf("(%T) unexpected amount of found objects %d", s, ln)
	}

	return res[0], nil
}

func readFullStream(s *Streamer, cap int) ([]*object.ID, error) {
	res := make([]*object.ID, 0, cap)

	for {
		r, err := s.Recv()
		if err != nil {
			if errors.Is(errors.Cause(err), io.EOF) {
				break
			}

			return nil, errors.Wrapf(err, "(%s) could not receive search result", "readFullStream")
		}

		res = append(res, r.IDList()...)
	}

	return res, nil
}

func NewRightChildSearcher(svc *Service) *RelationSearcher {
	return &RelationSearcher{
		svc: svc,
		queryGenerator: func(addr *object.Address) query.Query {
			return queryV1.NewRightChildQuery(addr.ObjectID())
		},
	}
}

func NewLinkingSearcher(svc *Service) *RelationSearcher {
	return &RelationSearcher{
		svc: svc,
		queryGenerator: func(addr *object.Address) query.Query {
			return queryV1.NewLinkingQuery(addr.ObjectID())
		},
	}
}
