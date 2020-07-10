package object

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/object"
	v1 "github.com/nspcc-dev/neofs-api-go/query"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"go.uber.org/zap"
)

// QueryFilter is a type alias of
// Filter from query package of neofs-api-go.
type QueryFilter = v1.Filter

const (
	// KeyChild is a filter key to child link.
	KeyChild = "CHILD"

	// KeyPrev is a filter key to previous link.
	KeyPrev = "PREV"

	// KeyNext is a filter key to next link.
	KeyNext = "NEXT"

	// KeyID is a filter key to object ID.
	KeyID = "ID"

	// KeyCID is a filter key to container ID.
	KeyCID = "CID"

	// KeyOwnerID is a filter key to owner ID.
	KeyOwnerID = "OWNERID"

	// KeyRootObject is a filter key to objects w/o parent links.
	KeyRootObject = "ROOT_OBJECT"
)

type (
	objectSearcher interface {
		searchObjects(context.Context, transport.SearchInfo) ([]Address, error)
	}

	coreObjectSearcher struct {
		executor operationExecutor
	}

	// objectAddressSet is and interface of object address set.
	objectAddressSet interface {
		responseItemHandler

		// list returns all elements of set.
		list() []Address
	}

	// coreObjAddrSet is and implementation of objectAddressSet interface used in Object service production.
	coreObjAddrSet struct {
		// Read-write mutex for race protection.
		*sync.RWMutex

		// Storing element of set.
		items []Address
	}
)

var addrPerMsg = int64(maxGetPayloadSize / new(Address).Size())

var (
	_ transport.SearchInfo = (*transportRequest)(nil)
	_ objectSearcher       = (*coreObjectSearcher)(nil)
	_ objectAddressSet     = (*coreObjAddrSet)(nil)
)

func (s *transportRequest) GetCID() CID { return s.serviceRequest.(*object.SearchRequest).CID() }

func (s *transportRequest) GetQuery() []byte {
	return s.serviceRequest.(*object.SearchRequest).GetQuery()
}

func (s *objectService) Search(req *object.SearchRequest, srv object.Service_SearchServer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error(panicLogMsg,
				zap.Stringer("request", object.RequestSearch),
				zap.Any("reason", r),
			)

			err = errServerPanic
		}

		err = s.statusCalculator.make(requestError{
			t: object.RequestSearch,
			e: err,
		})
	}()

	var r interface{}

	if r, err = s.requestHandler.handleRequest(srv.Context(), handleRequestParams{
		request:  req,
		executor: s,
	}); err != nil {
		return err
	}

	addrList := r.([]Address)

	for {
		cut := min(int64(len(addrList)), addrPerMsg)

		resp := makeSearchResponse(addrList[:cut])
		if err = s.respPreparer.prepareResponse(srv.Context(), req, resp); err != nil {
			return
		}

		if err = srv.Send(resp); err != nil {
			return
		}

		addrList = addrList[cut:]
		if len(addrList) == 0 {
			break
		}
	}

	return err
}

func (s *coreObjectSearcher) searchObjects(ctx context.Context, sInfo transport.SearchInfo) ([]Address, error) {
	addrSet := newUniqueAddressAccumulator()
	if err := s.executor.executeOperation(ctx, sInfo, addrSet); err != nil {
		return nil, err
	}

	return addrSet.list(), nil
}

func newUniqueAddressAccumulator() objectAddressSet {
	return &coreObjAddrSet{
		RWMutex: new(sync.RWMutex),
		items:   make([]Address, 0, 10),
	}
}

func (s *coreObjAddrSet) handleItem(v interface{}) {
	addrList := v.([]Address)

	s.Lock()

loop:
	for i := range addrList {
		for j := range s.items {
			if s.items[j].Equal(&addrList[i]) {
				continue loop
			}
		}
		s.items = append(s.items, addrList[i])
	}

	s.Unlock()
}

func (s *coreObjAddrSet) list() []Address {
	s.RLock()
	defer s.RUnlock()

	return s.items
}
