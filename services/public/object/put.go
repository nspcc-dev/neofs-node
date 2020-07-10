package object

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/session"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/nspcc-dev/neofs-node/lib/objutil"
	"github.com/nspcc-dev/neofs-node/lib/transformer"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	objectStorer interface {
		putObject(context.Context, transport.PutInfo) (*Address, error)
	}

	bifurcatingObjectStorer struct {
		straightStorer objectStorer
		tokenStorer    objectStorer
	}

	receivingObjectStorer struct {
		straightStorer objectStorer
		vPayload       objutil.Verifier
	}

	filteringObjectStorer struct {
		filter    Filter
		objStorer objectStorer
	}

	tokenObjectStorer struct {
		tokenStore session.PrivateTokenStore
		objStorer  objectStorer
	}

	transformingObjectStorer struct {
		transformer transformer.Transformer
		objStorer   objectStorer

		// Set of errors that won't be converted to errTransformer
		mErr map[error]struct{}
	}

	straightObjectStorer struct {
		executor operationExecutor
	}

	putRequest struct {
		*object.PutRequest
		srv     object.Service_PutServer
		timeout time.Duration
	}

	addressAccumulator interface {
		responseItemHandler
		address() *Address
	}

	coreAddrAccum struct {
		*sync.Once
		addr *Address
	}

	rawPutInfo struct {
		*rawMetaInfo
		obj     *Object
		r       io.Reader
		copyNum uint32
	}

	putStreamReader struct {
		tail []byte
		srv  object.Service_PutServer
	}
)

type transformerHandlerErr struct {
	error
}

const (
	errObjectExpected = internal.Error("missing object")
	errChunkExpected  = internal.Error("empty chunk received")
)

const (
	errMissingOwnerKeys  = internal.Error("missing owner keys")
	errBrokenToken       = internal.Error("broken token structure")
	errNilToken          = internal.Error("missing session token")
	errWrongTokenAddress = internal.Error("wrong object address in token")
)

const errTransformer = internal.Error("could not transform the object")

var (
	_ transport.PutInfo  = (*rawPutInfo)(nil)
	_ addressAccumulator = (*coreAddrAccum)(nil)
	_ objectStorer       = (*straightObjectStorer)(nil)
	_ transport.PutInfo  = (*putRequest)(nil)
	_ io.Reader          = (*putStreamReader)(nil)
	_ objectStorer       = (*filteringObjectStorer)(nil)
	_ objectStorer       = (*transformingObjectStorer)(nil)
	_ objectStorer       = (*tokenObjectStorer)(nil)
	_ objectStorer       = (*receivingObjectStorer)(nil)
)

func (s *objectService) Put(srv object.Service_PutServer) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error(panicLogMsg,
				zap.Stringer("request", object.RequestPut),
				zap.Any("reason", r),
			)

			err = errServerPanic
		}

		err = s.statusCalculator.make(requestError{
			t: object.RequestPut,
			e: err,
		})
	}()

	var req *object.PutRequest

	if req, err = recvPutHeaderMsg(srv); err != nil {
		return
	}

	_, err = s.requestHandler.handleRequest(srv.Context(), handleRequestParams{
		request: &putRequest{
			PutRequest: req,
			srv:        srv,
		},
		executor: s,
	})

	return err
}

func (s *bifurcatingObjectStorer) putObject(ctx context.Context, info transport.PutInfo) (*Address, error) {
	if withTokenFromOwner(info) {
		return s.tokenStorer.putObject(ctx, info)
	}

	return s.straightStorer.putObject(ctx, info)
}

func withTokenFromOwner(src service.SessionTokenSource) bool {
	if src == nil {
		return false
	}

	token := src.GetSessionToken()
	if token == nil {
		return false
	}

	signedReq, ok := src.(service.SignKeyPairSource)
	if !ok {
		return false
	}

	signKeyPairs := signedReq.GetSignKeyPairs()
	if len(signKeyPairs) == 0 {
		return false
	}

	firstKey := signKeyPairs[0].GetPublicKey()
	if firstKey == nil {
		return false
	}

	reqOwner, err := refs.NewOwnerID(firstKey)
	if err != nil {
		return false
	}

	return reqOwner.Equal(token.GetOwnerID())
}

func (s *tokenObjectStorer) putObject(ctx context.Context, info transport.PutInfo) (*Address, error) {
	token := info.GetSessionToken()

	key := session.PrivateTokenKey{}
	key.SetOwnerID(token.GetOwnerID())
	key.SetTokenID(token.GetID())

	pToken, err := s.tokenStore.Fetch(key)
	if err != nil {
		return nil, &detailedError{
			error: errTokenRetrieval,
			d:     privateTokenRecvDetails(token.GetID(), token.GetOwnerID()),
		}
	}

	return s.objStorer.putObject(
		contextWithValues(ctx,
			transformer.PrivateSessionToken, pToken,
			transformer.PublicSessionToken, token,
			implementations.BearerToken, info.GetBearerToken(),
			implementations.ExtendedHeaders, info.ExtendedHeaders(),
		),
		info,
	)
}

func (s *filteringObjectStorer) putObject(ctx context.Context, info transport.PutInfo) (*Address, error) {
	if res := s.filter.Pass(
		contextWithValues(ctx, ttlValue, info.GetTTL()),
		&Meta{Object: info.GetHead()},
	); res.Code() != localstore.CodePass {
		if err := res.Err(); err != nil {
			return nil, err
		}

		return nil, errObjectFilter
	}

	return s.objStorer.putObject(ctx, info)
}

func (s *receivingObjectStorer) putObject(ctx context.Context, src transport.PutInfo) (*Address, error) {
	obj := src.GetHead()
	obj.Payload = make([]byte, obj.SystemHeader.PayloadLength)

	if _, err := io.ReadFull(src.Payload(), obj.Payload); err != nil && err != io.EOF {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = transformer.ErrPayloadEOF
		}

		return nil, err
	} else if err = s.vPayload.Verify(ctx, obj); err != nil {
		return nil, errPayloadChecksum
	}

	putInfo := newRawPutInfo()
	putInfo.setTimeout(src.GetTimeout())
	putInfo.setTTL(src.GetTTL())
	putInfo.setCopiesNumber(src.CopiesNumber())
	putInfo.setHead(obj)
	putInfo.setSessionToken(src.GetSessionToken())
	putInfo.setBearerToken(src.GetBearerToken())
	putInfo.setExtendedHeaders(src.ExtendedHeaders())

	return s.straightStorer.putObject(ctx, putInfo)
}

func (s *transformingObjectStorer) putObject(ctx context.Context, src transport.PutInfo) (res *Address, err error) {
	var (
		ttl     = src.GetTTL()
		timeout = src.GetTimeout()
		copyNum = src.CopiesNumber()
		token   = src.GetSessionToken()
		bearer  = src.GetBearerToken()
		extHdrs = src.ExtendedHeaders()
	)

	err = s.transformer.Transform(ctx,
		transformer.ProcUnit{
			Head:    src.GetHead(),
			Payload: src.Payload(),
		}, func(ctx context.Context, unit transformer.ProcUnit) error {
			res = unit.Head.Address()

			putInfo := newRawPutInfo()
			putInfo.setHead(unit.Head)
			putInfo.setPayload(unit.Payload)
			putInfo.setTimeout(timeout)
			putInfo.setTTL(ttl)
			putInfo.setCopiesNumber(copyNum)
			putInfo.setSessionToken(token)
			putInfo.setBearerToken(bearer)
			putInfo.setExtendedHeaders(extHdrs)

			_, err := s.objStorer.putObject(ctx, putInfo)
			if err != nil {
				err = &transformerHandlerErr{
					error: err,
				}
			}
			return err
		},
	)

	if e := errors.Cause(err); e != nil {
		if v, ok := e.(*transformerHandlerErr); ok {
			err = v.error
		} else if _, ok := s.mErr[e]; !ok {
			err = errTransformer
		}
	}

	return res, err
}

func (s *putStreamReader) Read(p []byte) (n int, err error) {
	if s.srv == nil {
		return 0, io.EOF
	}

	n += copy(p, s.tail)
	if n > 0 {
		s.tail = s.tail[n:]
		return
	}

	var msg *object.PutRequest

	if msg, err = s.srv.Recv(); err != nil {
		return
	}

	chunk := msg.GetChunk()
	if len(chunk) == 0 {
		return 0, errChunkExpected
	}

	r := copy(p, chunk)

	s.tail = chunk[r:]

	n += r

	return
}

func (s *straightObjectStorer) putObject(ctx context.Context, pInfo transport.PutInfo) (*Address, error) {
	addrAccum := newAddressAccumulator()
	if err := s.executor.executeOperation(ctx, pInfo, addrAccum); err != nil {
		return nil, err
	}

	return addrAccum.address(), nil
}

func recvPutHeaderMsg(srv object.Service_PutServer) (*object.PutRequest, error) {
	req, err := srv.Recv()
	if err != nil {
		return nil, err
	} else if req == nil {
		return nil, errHeaderExpected
	} else if h := req.GetHeader(); h == nil {
		return nil, errHeaderExpected
	} else if h.GetObject() == nil {
		return nil, errObjectExpected
	}

	return req, nil
}

func contextWithValues(parentCtx context.Context, items ...interface{}) context.Context {
	fCtx := parentCtx
	for i := 0; i < len(items); i += 2 {
		fCtx = context.WithValue(fCtx, items[i], items[i+1])
	}

	return fCtx
}

func (s *putRequest) GetTimeout() time.Duration { return s.timeout }

func (s *putRequest) GetHead() *Object { return s.GetHeader().GetObject() }

func (s *putRequest) CopiesNumber() uint32 {
	h := s.GetHeader()
	if h == nil {
		return 0
	}

	return h.GetCopiesNumber()
}

func (s *putRequest) Payload() io.Reader {
	return &putStreamReader{
		srv: s.srv,
	}
}

func (s *rawPutInfo) GetHead() *Object {
	return s.obj
}

func (s *rawPutInfo) setHead(obj *Object) {
	s.obj = obj
}

func (s *rawPutInfo) Payload() io.Reader {
	return s.r
}

func (s *rawPutInfo) setPayload(r io.Reader) {
	s.r = r
}

func (s *rawPutInfo) CopiesNumber() uint32 {
	return s.copyNum
}

func (s *rawPutInfo) setCopiesNumber(v uint32) {
	s.copyNum = v
}

func (s *rawPutInfo) getMetaInfo() *rawMetaInfo {
	return s.rawMetaInfo
}

func (s *rawPutInfo) setMetaInfo(v *rawMetaInfo) {
	s.rawMetaInfo = v
	s.setType(object.RequestPut)
}

func newRawPutInfo() *rawPutInfo {
	res := new(rawPutInfo)

	res.setMetaInfo(newRawMetaInfo())

	return res
}

func (s *coreAddrAccum) handleItem(item interface{}) { s.Do(func() { s.addr = item.(*Address) }) }

func (s *coreAddrAccum) address() *Address { return s.addr }

func newAddressAccumulator() addressAccumulator { return &coreAddrAccum{Once: new(sync.Once)} }
