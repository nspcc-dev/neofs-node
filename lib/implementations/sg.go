package implementations

import (
	"context"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/storagegroup"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// StorageGroupInfoReceiverParams groups the parameters of
	// storage group information receiver.
	StorageGroupInfoReceiverParams struct {
		SelectiveContainerExecutor SelectiveContainerExecutor
		Logger                     *zap.Logger
	}

	sgInfoRecv struct {
		executor SelectiveContainerExecutor
		log      *zap.Logger
	}
)

const locationFinderInstanceFailMsg = "could not create object location finder"

// ErrIncompleteSGInfo is returned by storage group information receiver
// that could not receive full information.
const ErrIncompleteSGInfo = internal.Error("could not receive full storage group info")

// PublicSessionToken is a context key for SessionToken.
// FIXME: temp solution for cycle import fix.
//  Unify with same const from transformer pkg.
const PublicSessionToken = "public token"

// BearerToken is a context key for BearerToken.
const BearerToken = "bearer token"

// ExtendedHeaders is a context key for X-headers.
const ExtendedHeaders = "extended headers"

func (s *sgInfoRecv) GetSGInfo(ctx context.Context, cid CID, group []ObjectID) (*storagegroup.StorageGroup, error) {
	var (
		err      error
		res      = new(storagegroup.StorageGroup)
		hashList = make([]hash.Hash, 0, len(group))
	)

	m := make(map[string]struct{}, len(group))
	for i := range group {
		m[group[i].String()] = struct{}{}
	}

	// FIXME: hardcoded for simplicity.
	//   Function is called in next cases:
	//    - SG transformation on trusted node side (only in this case session token is needed);
	//    - SG info check on container nodes (token is not needed since system group has extra access);
	//    - data audit on inner ring nodes (same as previous).
	var token service.SessionToken
	if v, ok := ctx.Value(PublicSessionToken).(service.SessionToken); ok {
		token = v
	}

	var bearer service.BearerToken
	if v, ok := ctx.Value(BearerToken).(service.BearerToken); ok {
		bearer = v
	}

	var extHdrs []service.ExtendedHeader
	if v, ok := ctx.Value(ExtendedHeaders).([]service.ExtendedHeader); ok {
		extHdrs = v
	}

	if err = s.executor.Head(ctx, &HeadParams{
		GetParams: GetParams{
			SelectiveParams: SelectiveParams{
				CID:    cid,
				TTL:    service.SingleForwardingTTL,
				IDList: group,
				Breaker: func(addr refs.Address) (cFlag ProgressControlFlag) {
					if len(m) == 0 {
						cFlag = BreakProgress
					} else if _, ok := m[addr.ObjectID.String()]; !ok {
						cFlag = NextAddress
					}
					return
				},
				Token: token,

				Bearer: bearer,

				ExtendedHeaders: extHdrs,
			},
			Handler: func(_ multiaddr.Multiaddr, obj *object.Object) {
				_, hashHeader := obj.LastHeader(object.HeaderType(object.HomoHashHdr))
				if hashHeader == nil {
					return
				}

				hashList = append(hashList, hashHeader.Value.(*object.Header_HomoHash).HomoHash)
				res.ValidationDataSize += obj.SystemHeader.PayloadLength
				delete(m, obj.SystemHeader.ID.String())
			},
		},
		FullHeaders: true,
	}); err != nil {
		return nil, err
	} else if len(m) > 0 {
		return nil, ErrIncompleteSGInfo
	}

	res.ValidationHash, err = hash.Concat(hashList)

	return res, err
}

// NewStorageGroupInfoReceiver constructs storagegroup.InfoReceiver from SelectiveContainerExecutor.
func NewStorageGroupInfoReceiver(p StorageGroupInfoReceiverParams) (storagegroup.InfoReceiver, error) {
	switch {
	case p.Logger == nil:
		return nil, errors.Wrap(errEmptyLogger, locationFinderInstanceFailMsg)
	case p.SelectiveContainerExecutor == nil:
		return nil, errors.Wrap(errEmptyObjectsContainerHandler, locationFinderInstanceFailMsg)
	}

	return &sgInfoRecv{
		executor: p.SelectiveContainerExecutor,
		log:      p.Logger,
	}, nil
}
