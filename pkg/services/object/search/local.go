package searchsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
	"github.com/pkg/errors"
)

type localStream struct {
	query query.Query

	storage *engine.StorageEngine

	cid *container.ID
}

func (s *localStream) stream(ctx context.Context, ch chan<- []*objectSDK.ID) error {
	fs := s.query.ToSearchFilters()

	addrList, err := engine.Select(s.storage, s.cid, fs)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not select objects from local storage", s)
	}

	idList := make([]*objectSDK.ID, 0, len(addrList))

	for i := range addrList {
		idList = append(idList, addrList[i].ObjectID())
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- idList:
		return nil
	}
}
