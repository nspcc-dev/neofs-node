package storage

import (
	"context"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/query"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/replication"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transport"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	locator struct {
		executor transport.SelectiveContainerExecutor
		log      *zap.Logger
	}

	// LocatorParams groups the parameters of ObjectLocator constructor.
	LocatorParams struct {
		SelectiveContainerExecutor transport.SelectiveContainerExecutor
		Logger                     *zap.Logger
	}
)

const locatorInstanceFailMsg = "could not create object locator"

var errEmptyObjectsContainerHandler = errors.New("empty container objects container handler")

func (s *locator) LocateObject(ctx context.Context, addr refs.Address) (res []multiaddr.Multiaddr, err error) {
	queryBytes, err := (&query.Query{
		Filters: []query.Filter{
			{
				Type:  query.Filter_Exact,
				Name:  transport.KeyID,
				Value: addr.ObjectID.String(),
			},
		},
	}).Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "locate object failed on query marshal")
	}

	err = s.executor.Search(ctx, &transport.SearchParams{
		SelectiveParams: transport.SelectiveParams{
			CID:    addr.CID,
			TTL:    service.NonForwardingTTL,
			IDList: make([]object.ID, 1),
		},
		SearchCID:   addr.CID,
		SearchQuery: queryBytes,
		Handler: func(node multiaddr.Multiaddr, addrList []refs.Address) {
			if len(addrList) > 0 {
				res = append(res, node)
			}
		},
	})

	return
}

// NewObjectLocator constructs replication.ObjectLocator from SelectiveContainerExecutor.
func NewObjectLocator(p LocatorParams) (replication.ObjectLocator, error) {
	switch {
	case p.SelectiveContainerExecutor == nil:
		return nil, errors.Wrap(errEmptyObjectsContainerHandler, locatorInstanceFailMsg)
	case p.Logger == nil:
		return nil, errors.Wrap(logger.ErrNilLogger, locatorInstanceFailMsg)
	}

	return &locator{
		executor: p.SelectiveContainerExecutor,
		log:      p.Logger,
	}, nil
}
