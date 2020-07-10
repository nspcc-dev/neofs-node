package object

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/transformer"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	objectRemover interface {
		delete(context.Context, deleteInfo) error
	}

	coreObjRemover struct {
		delPrep     deletePreparer
		straightRem objectRemover
		tokenStore  session.PrivateTokenStore

		// Set of potential deletePreparer errors that won't be converted into errDeletePrepare
		mErr map[error]struct{}

		log *zap.Logger
	}

	straightObjRemover struct {
		tombCreator tombstoneCreator
		objStorer   objectStorer
	}

	tombstoneCreator interface {
		createTombstone(context.Context, deleteInfo) *Object
	}

	coreTombCreator struct{}

	deletePreparer interface {
		prepare(context.Context, deleteInfo) ([]deleteInfo, error)
	}

	coreDelPreparer struct {
		timeout     time.Duration
		childLister objectChildrenLister
	}

	deleteInfo interface {
		transport.AddressInfo
		GetOwnerID() OwnerID
	}

	rawDeleteInfo struct {
		rawAddrInfo
		ownerID OwnerID
	}
)

const emRemovePart = "could not remove object part #%d of #%d"

var (
	_ tombstoneCreator = (*coreTombCreator)(nil)
	_ deleteInfo       = (*rawDeleteInfo)(nil)
	_ deletePreparer   = (*coreDelPreparer)(nil)
	_ objectRemover    = (*straightObjRemover)(nil)
	_ objectRemover    = (*coreObjRemover)(nil)
	_ deleteInfo       = (*transportRequest)(nil)

	checksumOfEmptyPayload = sha256.Sum256([]byte{})
)

func (s *objectService) Delete(ctx context.Context, req *object.DeleteRequest) (res *object.DeleteResponse, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error(panicLogMsg,
				zap.Stringer("request", object.RequestDelete),
				zap.Any("reason", r),
			)

			err = errServerPanic
		}

		err = s.statusCalculator.make(requestError{
			t: object.RequestDelete,
			e: err,
		})
	}()

	if _, err = s.requestHandler.handleRequest(ctx, handleRequestParams{
		request:  req,
		executor: s,
	}); err != nil {
		return
	}

	res = makeDeleteResponse()
	err = s.respPreparer.prepareResponse(ctx, req, res)

	return
}

func (s *coreObjRemover) delete(ctx context.Context, dInfo deleteInfo) error {
	token := dInfo.GetSessionToken()
	if token == nil {
		return errNilToken
	}

	key := session.PrivateTokenKey{}
	key.SetOwnerID(dInfo.GetOwnerID())
	key.SetTokenID(token.GetID())

	pToken, err := s.tokenStore.Fetch(key)
	if err != nil {
		return &detailedError{
			error: errTokenRetrieval,
			d:     privateTokenRecvDetails(token.GetID(), token.GetOwnerID()),
		}
	}

	deleteList, err := s.delPrep.prepare(ctx, dInfo)
	if err != nil {
		if _, ok := s.mErr[errors.Cause(err)]; !ok {
			s.log.Error("delete info preparation failure",
				zap.String("error", err.Error()),
			)

			err = errDeletePrepare
		}

		return err
	}

	ctx = contextWithValues(ctx,
		transformer.PrivateSessionToken, pToken,
		transformer.PublicSessionToken, token,
		implementations.BearerToken, dInfo.GetBearerToken(),
		implementations.ExtendedHeaders, dInfo.ExtendedHeaders(),
	)

	for i := range deleteList {
		if err := s.straightRem.delete(ctx, deleteList[i]); err != nil {
			return errors.Wrapf(err, emRemovePart, i+1, len(deleteList))
		}
	}

	return nil
}

func (s *coreDelPreparer) prepare(ctx context.Context, src deleteInfo) ([]deleteInfo, error) {
	var (
		ownerID = src.GetOwnerID()
		token   = src.GetSessionToken()
		addr    = src.GetAddress()
		bearer  = src.GetBearerToken()
		extHdrs = src.ExtendedHeaders()
	)

	dInfo := newRawDeleteInfo()
	dInfo.setOwnerID(ownerID)
	dInfo.setAddress(addr)
	dInfo.setTTL(service.NonForwardingTTL)
	dInfo.setSessionToken(token)
	dInfo.setBearerToken(bearer)
	dInfo.setExtendedHeaders(extHdrs)
	dInfo.setTimeout(s.timeout)

	ctx = contextWithValues(ctx,
		transformer.PublicSessionToken, src.GetSessionToken(),
		implementations.BearerToken, bearer,
		implementations.ExtendedHeaders, extHdrs,
	)

	children := s.childLister.children(ctx, addr)

	res := make([]deleteInfo, 0, len(children)+1)

	res = append(res, dInfo)

	for i := range children {
		dInfo = newRawDeleteInfo()
		dInfo.setOwnerID(ownerID)
		dInfo.setAddress(Address{
			ObjectID: children[i],
			CID:      addr.CID,
		})
		dInfo.setTTL(service.NonForwardingTTL)
		dInfo.setSessionToken(token)
		dInfo.setBearerToken(bearer)
		dInfo.setExtendedHeaders(extHdrs)
		dInfo.setTimeout(s.timeout)

		res = append(res, dInfo)
	}

	return res, nil
}

func (s *straightObjRemover) delete(ctx context.Context, dInfo deleteInfo) error {
	putInfo := newRawPutInfo()
	putInfo.setHead(
		s.tombCreator.createTombstone(ctx, dInfo),
	)
	putInfo.setSessionToken(dInfo.GetSessionToken())
	putInfo.setBearerToken(dInfo.GetBearerToken())
	putInfo.setExtendedHeaders(dInfo.ExtendedHeaders())
	putInfo.setTTL(dInfo.GetTTL())
	putInfo.setTimeout(dInfo.GetTimeout())

	_, err := s.objStorer.putObject(ctx, putInfo)

	return err
}

func (s *coreTombCreator) createTombstone(ctx context.Context, dInfo deleteInfo) *Object {
	addr := dInfo.GetAddress()
	obj := &Object{
		SystemHeader: SystemHeader{
			ID:      addr.ObjectID,
			CID:     addr.CID,
			OwnerID: dInfo.GetOwnerID(),
		},
		Headers: []Header{
			{
				Value: &object.Header_Tombstone{
					Tombstone: new(object.Tombstone),
				},
			},
			{
				Value: &object.Header_PayloadChecksum{
					PayloadChecksum: checksumOfEmptyPayload[:],
				},
			},
		},
	}

	return obj
}

func (s *rawDeleteInfo) GetAddress() Address {
	return s.addr
}

func (s *rawDeleteInfo) setAddress(addr Address) {
	s.addr = addr
}

func (s *rawDeleteInfo) GetOwnerID() OwnerID {
	return s.ownerID
}

func (s *rawDeleteInfo) setOwnerID(id OwnerID) {
	s.ownerID = id
}

func (s *rawDeleteInfo) setAddrInfo(v *rawAddrInfo) {
	s.rawAddrInfo = *v
	s.setType(object.RequestDelete)
}

func newRawDeleteInfo() *rawDeleteInfo {
	res := new(rawDeleteInfo)

	res.setAddrInfo(newRawAddressInfo())

	return res
}

func (s *transportRequest) GetToken() *session.Token {
	return s.serviceRequest.(*object.DeleteRequest).GetToken()
}
func (s *transportRequest) GetHead() *Object {
	return &Object{SystemHeader: SystemHeader{
		ID: s.serviceRequest.(*object.DeleteRequest).Address.ObjectID,
	}}
}

func (s *transportRequest) GetOwnerID() OwnerID {
	return s.serviceRequest.(*object.DeleteRequest).OwnerID
}
