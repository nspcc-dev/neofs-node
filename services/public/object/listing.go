package object

import (
	"context"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/object"
	v1 "github.com/nspcc-dev/neofs-api-go/query"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/objio"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"go.uber.org/zap"
)

type (
	objectChildrenLister interface {
		children(context.Context, Address) []ID
	}

	coreChildrenLister struct {
		queryFn     relationQueryFunc
		objSearcher objectSearcher
		log         *zap.Logger
		timeout     time.Duration
	}

	relationQueryFunc func(Address) ([]byte, error)

	rawSearchInfo struct {
		*rawMetaInfo
		cid   CID
		query []byte
	}

	neighborReceiver struct {
		firstChildQueryFn    relationQueryFunc
		leftNeighborQueryFn  relationQueryFunc
		rightNeighborQueryFn relationQueryFunc
		rangeDescRecv        selectiveRangeReceiver
	}

	selectiveRangeReceiver interface {
		rangeDescriptor(context.Context, Address, relationQueryFunc) (RangeDescriptor, error)
	}

	selectiveRangeRecv struct {
		executor implementations.SelectiveContainerExecutor
	}
)

const (
	lmQueryMarshalFail = "marshal search query failure"
	lmListFail         = "searching inside children listing failure"

	errRelationNotFound = internal.Error("relation not found")
)

var (
	_ relationQueryFunc      = coreChildrenQueryFunc
	_ transport.SearchInfo   = (*rawSearchInfo)(nil)
	_ objectChildrenLister   = (*coreChildrenLister)(nil)
	_ objio.RelativeReceiver = (*neighborReceiver)(nil)
	_ selectiveRangeReceiver = (*selectiveRangeRecv)(nil)
)

func (s *neighborReceiver) Base(ctx context.Context, addr Address) (RangeDescriptor, error) {
	if res, err := s.rangeDescRecv.rangeDescriptor(ctx, addr, s.firstChildQueryFn); err == nil {
		return res, nil
	}

	return s.rangeDescRecv.rangeDescriptor(ctx, addr, nil)
}

func (s *neighborReceiver) Neighbor(ctx context.Context, addr Address, left bool) (res RangeDescriptor, err error) {
	if left {
		res, err = s.rangeDescRecv.rangeDescriptor(ctx, addr, s.leftNeighborQueryFn)
	} else {
		res, err = s.rangeDescRecv.rangeDescriptor(ctx, addr, s.rightNeighborQueryFn)
	}

	return
}

func (s *selectiveRangeRecv) rangeDescriptor(ctx context.Context, addr Address, fn relationQueryFunc) (res RangeDescriptor, err error) {
	b := false

	p := &implementations.HeadParams{
		GetParams: implementations.GetParams{
			SelectiveParams: implementations.SelectiveParams{
				CID:        addr.CID,
				ServeLocal: true,
				TTL:        service.SingleForwardingTTL,
				Token:      tokenFromContext(ctx),
				Bearer:     bearerFromContext(ctx),

				ExtendedHeaders: extendedHeadersFromContext(ctx),
			},
			Handler: func(_ multiaddr.Multiaddr, obj *Object) {
				res.Addr = *obj.Address()
				res.Offset = 0
				res.Size = int64(obj.SystemHeader.PayloadLength)

				sameID := res.Addr.ObjectID.Equal(addr.ObjectID)
				bound := boundaryChild(obj)
				res.LeftBound = sameID || bound == boundBoth || bound == boundLeft
				res.RightBound = sameID || bound == boundBoth || bound == boundRight

				b = true
			},
		},
		FullHeaders: true,
	}

	if fn != nil {
		if p.Query, err = fn(addr); err != nil {
			return
		}
	} else {
		p.IDList = []ID{addr.ObjectID}
	}

	if err = s.executor.Head(ctx, p); err != nil {
		return
	} else if !b {
		err = errRelationNotFound
	}

	return res, err
}

const (
	boundBoth = iota
	boundLeft
	boundRight
	boundMid
)

func boundaryChild(obj *Object) (res int) {
	splitInd, _ := obj.LastHeader(object.HeaderType(object.TransformHdr))
	if splitInd < 0 {
		return
	}

	for i := len(obj.Headers) - 1; i > splitInd; i-- {
		hVal := obj.Headers[i].GetValue()
		if hVal == nil {
			continue
		}

		hLink, ok := hVal.(*object.Header_Link)
		if !ok || hLink == nil || hLink.Link == nil {
			continue
		}

		linkType := hLink.Link.GetType()
		if linkType != object.Link_Previous && linkType != object.Link_Next {
			continue
		}

		res = boundMid

		if hLink.Link.ID.Empty() {
			if linkType == object.Link_Next {
				res = boundRight
			} else if linkType == object.Link_Previous {
				res = boundLeft
			}

			return
		}
	}

	return res
}

func firstChildQueryFunc(addr Address) ([]byte, error) {
	return (&v1.Query{
		Filters: append(parentFilters(addr), QueryFilter{
			Type:  v1.Filter_Exact,
			Name:  KeyPrev,
			Value: ID{}.String(),
		}),
	}).Marshal()
}

func leftNeighborQueryFunc(addr Address) ([]byte, error) {
	return idQueryFunc(KeyNext, addr.ObjectID)
}

func rightNeighborQueryFunc(addr Address) ([]byte, error) {
	return idQueryFunc(KeyPrev, addr.ObjectID)
}

func idQueryFunc(key string, id ID) ([]byte, error) {
	return (&v1.Query{Filters: []QueryFilter{
		{
			Type:  v1.Filter_Exact,
			Name:  key,
			Value: id.String(),
		},
	}}).Marshal()
}

func coreChildrenQueryFunc(addr Address) ([]byte, error) {
	return (&v1.Query{Filters: parentFilters(addr)}).Marshal()
}

func (s *coreChildrenLister) children(ctx context.Context, parent Address) []ID {
	query, err := s.queryFn(parent)
	if err != nil {
		s.log.Error(lmQueryMarshalFail, zap.Error(err))
		return nil
	}

	sInfo := newRawSearchInfo()
	sInfo.setTTL(service.NonForwardingTTL)
	sInfo.setTimeout(s.timeout)
	sInfo.setCID(parent.CID)
	sInfo.setQuery(query)
	sInfo.setSessionToken(tokenFromContext(ctx))
	sInfo.setBearerToken(bearerFromContext(ctx))
	sInfo.setExtendedHeaders(extendedHeadersFromContext(ctx))

	children, err := s.objSearcher.searchObjects(ctx, sInfo)
	if err != nil {
		s.log.Error(lmListFail, zap.Error(err))
		return nil
	}

	res := make([]ID, 0, len(children))
	for i := range children {
		res = append(res, children[i].ObjectID)
	}

	return res
}

func (s *rawSearchInfo) GetCID() CID {
	return s.cid
}

func (s *rawSearchInfo) setCID(v CID) {
	s.cid = v
}

func (s *rawSearchInfo) GetQuery() []byte {
	return s.query
}

func (s *rawSearchInfo) setQuery(v []byte) {
	s.query = v
}

func (s *rawSearchInfo) getMetaInfo() *rawMetaInfo {
	return s.rawMetaInfo
}

func (s *rawSearchInfo) setMetaInfo(v *rawMetaInfo) {
	s.rawMetaInfo = v
	s.setType(object.RequestSearch)
}

func newRawSearchInfo() *rawSearchInfo {
	res := new(rawSearchInfo)

	res.setMetaInfo(newRawMetaInfo())

	return res
}

func parentFilters(addr Address) []QueryFilter {
	return []QueryFilter{
		{
			Type: v1.Filter_Exact,
			Name: transport.KeyHasParent,
		},
		{
			Type:  v1.Filter_Exact,
			Name:  transport.KeyParent,
			Value: addr.ObjectID.String(),
		},
	}
}
