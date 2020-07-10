package object

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"time"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/session"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/transport"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// MultiTransportParams groups the parameters for object transport component's constructor.
	MultiTransportParams struct {
		AddressStore     implementations.AddressStoreComponent
		EpochReceiver    EpochReceiver
		RemoteService    RemoteService
		Logger           *zap.Logger
		Key              *ecdsa.PrivateKey
		PutTimeout       time.Duration
		GetTimeout       time.Duration
		HeadTimeout      time.Duration
		SearchTimeout    time.Duration
		RangeHashTimeout time.Duration
		DialTimeout      time.Duration

		PrivateTokenStore session.PrivateTokenStore
	}

	transportComponent struct {
		reqSender requestSender

		resTracker resultTracker

		getCaller       remoteProcessCaller
		putCaller       remoteProcessCaller
		headCaller      remoteProcessCaller
		rangeCaller     remoteProcessCaller
		rangeHashCaller remoteProcessCaller
		searchCaller    remoteProcessCaller
	}

	requestSender interface {
		sendRequest(context.Context, sendParams) (interface{}, error)
	}

	sendParams struct {
		req     transport.MetaInfo
		node    multiaddr.Multiaddr
		handler remoteProcessCaller
	}

	clientInfo struct {
		sc  object.ServiceClient
		key *ecdsa.PublicKey
	}

	remoteProcessCaller interface {
		call(context.Context, serviceRequest, *clientInfo) (interface{}, error)
	}

	getCaller struct {
	}

	putCaller struct {
	}

	headCaller struct {
	}

	rangeCaller struct {
	}

	rangeHashCaller struct {
	}

	searchCaller struct {
	}

	coreRequestSender struct {
		requestPrep   transportRequestPreparer
		addressStore  implementations.AddressStoreComponent
		remoteService RemoteService

		putTimeout       time.Duration
		getTimeout       time.Duration
		searchTimeout    time.Duration
		headTimeout      time.Duration
		rangeHashTimeout time.Duration
		dialTimeout      time.Duration
	}

	signingFunc func(*ecdsa.PrivateKey, service.RequestSignedData) error

	coreRequestPreparer struct {
		epochRecv   EpochReceiver
		key         *ecdsa.PrivateKey
		signingFunc signingFunc

		privateTokenStore session.PrivateTokenSource
	}

	transportRequestPreparer interface {
		prepareRequest(transport.MetaInfo) (serviceRequest, error)
	}

	transportRequest struct {
		serviceRequest
		timeout time.Duration
	}

	putRequestSequence struct {
		*object.PutRequest
		chunks []*object.PutRequest
	}

	rawMetaInfo struct {
		raw     bool
		ttl     uint32
		timeout time.Duration
		token   service.SessionToken
		rt      object.RequestType
		bearer  service.BearerToken
		extHdrs []service.ExtendedHeader
	}

	rawAddrInfo struct {
		*rawMetaInfo
		addr Address
	}
)

const (
	minRemoteRequestTimeout = 5 * time.Second
	minDialTimeout          = 500 * time.Millisecond
)

const pmWrongRequestType = "unknown type: %T"

var (
	_ serviceRequest            = (*putRequestSequence)(nil)
	_ transport.MetaInfo        = (*transportRequest)(nil)
	_ requestSender             = (*coreRequestSender)(nil)
	_ transport.ObjectTransport = (*transportComponent)(nil)
	_ transportRequestPreparer  = (*coreRequestPreparer)(nil)
	_ transport.MetaInfo        = (*rawMetaInfo)(nil)
	_ transport.AddressInfo     = (*rawAddrInfo)(nil)

	_ remoteProcessCaller = (*getCaller)(nil)
	_ remoteProcessCaller = (*putCaller)(nil)
	_ remoteProcessCaller = (*headCaller)(nil)
	_ remoteProcessCaller = (*searchCaller)(nil)
	_ remoteProcessCaller = (*rangeCaller)(nil)
	_ remoteProcessCaller = (*rangeHashCaller)(nil)
)

func newRawMetaInfo() *rawMetaInfo {
	return new(rawMetaInfo)
}

func (s *rawMetaInfo) GetTTL() uint32 {
	return s.ttl
}

func (s *rawMetaInfo) setTTL(ttl uint32) {
	s.ttl = ttl
}

func (s *rawMetaInfo) GetTimeout() time.Duration {
	return s.timeout
}

func (s *rawMetaInfo) setTimeout(dur time.Duration) {
	s.timeout = dur
}

func (s *rawMetaInfo) GetSessionToken() service.SessionToken {
	return s.token
}

func (s *rawMetaInfo) setSessionToken(token service.SessionToken) {
	s.token = token
}

func (s *rawMetaInfo) GetBearerToken() service.BearerToken {
	return s.bearer
}

func (s *rawMetaInfo) setBearerToken(token service.BearerToken) {
	s.bearer = token
}

func (s *rawMetaInfo) ExtendedHeaders() []service.ExtendedHeader {
	return s.extHdrs
}

func (s *rawMetaInfo) setExtendedHeaders(v []service.ExtendedHeader) {
	s.extHdrs = v
}

func (s *rawMetaInfo) GetRaw() bool {
	return s.raw
}

func (s *rawMetaInfo) setRaw(raw bool) {
	s.raw = raw
}

func (s *rawMetaInfo) Type() object.RequestType {
	return s.rt
}

func (s *rawMetaInfo) setType(rt object.RequestType) {
	s.rt = rt
}

func (s *rawAddrInfo) GetAddress() Address {
	return s.addr
}

func (s *rawAddrInfo) setAddress(addr Address) {
	s.addr = addr
}

func (s *rawAddrInfo) getMetaInfo() *rawMetaInfo {
	return s.rawMetaInfo
}

func (s *rawAddrInfo) setMetaInfo(v *rawMetaInfo) {
	s.rawMetaInfo = v
}

func newRawAddressInfo() *rawAddrInfo {
	res := new(rawAddrInfo)

	res.setMetaInfo(newRawMetaInfo())

	return res
}

func (s *transportRequest) GetTimeout() time.Duration { return s.timeout }

func (s *transportComponent) Transport(ctx context.Context, p transport.ObjectTransportParams) {
	res, err := s.sendRequest(ctx, p.TransportInfo, p.TargetNode)
	p.ResultHandler.HandleResult(ctx, p.TargetNode, res, err)

	go s.resTracker.trackResult(ctx, resultItems{
		requestType:  p.TransportInfo.Type(),
		node:         p.TargetNode,
		satisfactory: err == nil,
	})
}

func (s *transportComponent) sendRequest(ctx context.Context, reqInfo transport.MetaInfo, node multiaddr.Multiaddr) (interface{}, error) {
	p := sendParams{
		req:  reqInfo,
		node: node,
	}

	switch reqInfo.Type() {
	case object.RequestSearch:
		p.handler = s.searchCaller
	case object.RequestPut:
		p.handler = s.putCaller
	case object.RequestHead:
		p.handler = s.headCaller
	case object.RequestGet:
		p.handler = s.getCaller
	case object.RequestRangeHash:
		p.handler = s.rangeHashCaller
	case object.RequestRange:
		p.handler = s.rangeCaller
	default:
		panic(fmt.Sprintf(pmWrongRequestType, reqInfo))
	}

	return s.reqSender.sendRequest(ctx, p)
}

func (s *searchCaller) call(ctx context.Context, r serviceRequest, c *clientInfo) (interface{}, error) {
	cSearch, err := c.sc.Search(ctx, r.(*object.SearchRequest))
	if err != nil {
		return nil, err
	}

	res := make([]Address, 0)

	for {
		r, err := cSearch.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		res = append(res, r.Addresses...)
	}

	return res, nil
}

func (s *rangeHashCaller) call(ctx context.Context, r serviceRequest, c *clientInfo) (interface{}, error) {
	resp, err := c.sc.GetRangeHash(ctx, r.(*object.GetRangeHashRequest))
	if err != nil {
		return nil, err
	}

	return resp.Hashes, nil
}

func (s *rangeCaller) call(ctx context.Context, r serviceRequest, c *clientInfo) (interface{}, error) {
	req := r.(*GetRangeRequest)

	resp, err := c.sc.GetRange(ctx, req)
	if err != nil {
		return nil, err
	}

	data := make([]byte, 0, req.Range.Length)

	for {
		resp, err := resp.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		data = append(data, resp.Fragment...)
	}

	return bytes.NewReader(data), nil
}

func (s *headCaller) call(ctx context.Context, r serviceRequest, c *clientInfo) (interface{}, error) {
	resp, err := c.sc.Head(ctx, r.(*object.HeadRequest))
	if err != nil {
		return nil, err
	}

	return resp.Object, nil
}

func (s *getCaller) call(ctx context.Context, r serviceRequest, c *clientInfo) (interface{}, error) {
	getClient, err := c.sc.Get(ctx, r.(*object.GetRequest))
	if err != nil {
		return nil, err
	}

	resp, err := getClient.Recv()
	if err != nil {
		return nil, err
	}

	obj := resp.GetObject()

	if resp.NotFull() {
		obj.Payload = make([]byte, 0, obj.SystemHeader.PayloadLength)

		for {
			resp, err := getClient.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}

				return nil, errors.Wrap(err, "get object received error")
			}

			obj.Payload = append(obj.Payload, resp.GetChunk()...)
		}
	}

	return obj, nil
}

func (s *putCaller) call(ctx context.Context, r serviceRequest, c *clientInfo) (interface{}, error) {
	putClient, err := c.sc.Put(ctx)
	if err != nil {
		return nil, err
	}

	req := r.(*putRequestSequence)

	if err := putClient.Send(req.PutRequest); err != nil {
		return nil, err
	}

	for i := range req.chunks {
		if err := putClient.Send(req.chunks[i]); err != nil {
			return nil, err
		}
	}

	resp, err := putClient.CloseAndRecv()
	if err != nil {
		return nil, err
	}

	return &resp.Address, nil
}

func (s *coreRequestPreparer) prepareRequest(req transport.MetaInfo) (serviceRequest, error) {
	var (
		signed bool
		tr     *transportRequest
		r      serviceRequest
	)

	if tr, signed = req.(*transportRequest); signed {
		r = tr.serviceRequest
	} else {
		switch req.Type() {
		case object.RequestSearch:
			r = prepareSearchRequest(req.(transport.SearchInfo))
		case object.RequestPut:
			r = preparePutRequest(req.(transport.PutInfo))
		case object.RequestGet:
			r = prepareGetRequest(req.(transport.GetInfo))
		case object.RequestHead:
			r = prepareHeadRequest(req.(transport.HeadInfo))
		case object.RequestRange:
			r = prepareRangeRequest(req.(transport.RangeInfo))
		case object.RequestRangeHash:
			r = prepareRangeHashRequest(req.(transport.RangeHashInfo))
		default:
			panic(fmt.Sprintf(pmWrongRequestType, req))
		}
	}

	r.SetTTL(req.GetTTL())
	r.SetEpoch(s.epochRecv.Epoch())
	r.SetRaw(req.GetRaw())
	r.SetBearer(
		toBearerMessage(
			req.GetBearerToken(),
		),
	)
	r.SetHeaders(
		toExtendedHeaderMessages(
			req.ExtendedHeaders(),
		),
	)

	if signed {
		return r, nil
	}

	key := s.key

	if token := req.GetSessionToken(); token != nil {
		/* FIXME: here we must determine whether the node is trusted,
		and if so, sign the request with a session key.
		In current implementation trusted node may lose its reputation
		in case of sending user requests in a nonexistent session.
		*/
		r.SetToken(toTokenMessage(token))

		privateTokenKey := session.PrivateTokenKey{}
		privateTokenKey.SetTokenID(token.GetID())
		privateTokenKey.SetOwnerID(token.GetOwnerID())

		pToken, err := s.privateTokenStore.Fetch(privateTokenKey)
		if err == nil {
			if err := signRequest(pToken.PrivateKey(), r); err != nil {
				return nil, err
			}
		}
	}

	return r, signRequest(key, r)
}

func toTokenMessage(token service.SessionToken) *service.Token {
	if token == nil {
		return nil
	} else if v, ok := token.(*service.Token); ok {
		return v
	}

	res := new(service.Token)

	res.SetID(token.GetID())
	res.SetOwnerID(token.GetOwnerID())
	res.SetVerb(token.GetVerb())
	res.SetAddress(token.GetAddress())
	res.SetCreationEpoch(token.CreationEpoch())
	res.SetExpirationEpoch(token.ExpirationEpoch())
	res.SetSessionKey(token.GetSessionKey())
	res.SetSignature(token.GetSignature())

	return res
}

func toBearerMessage(token service.BearerToken) *service.BearerTokenMsg {
	if token == nil {
		return nil
	} else if v, ok := token.(*service.BearerTokenMsg); ok {
		return v
	}

	res := new(service.BearerTokenMsg)

	res.SetACLRules(token.GetACLRules())
	res.SetOwnerID(token.GetOwnerID())
	res.SetExpirationEpoch(token.ExpirationEpoch())
	res.SetOwnerKey(token.GetOwnerKey())
	res.SetSignature(token.GetSignature())

	return res
}

func toExtendedHeaderMessages(hs []service.ExtendedHeader) []service.RequestExtendedHeader_KV {
	res := make([]service.RequestExtendedHeader_KV, 0, len(hs))

	for i := range hs {
		if hs[i] == nil {
			continue
		}

		h := service.RequestExtendedHeader_KV{}
		h.SetK(hs[i].Key())
		h.SetV(hs[i].Value())

		res = append(res, h)
	}

	return res
}

func signRequest(key *ecdsa.PrivateKey, req serviceRequest) error {
	signKeys := req.GetSignKeyPairs()
	ln := len(signKeys)

	// TODO: public key bytes can be stored in struct once
	if ln > 0 && bytes.Equal(
		crypto.MarshalPublicKey(signKeys[ln-1].GetPublicKey()),
		crypto.MarshalPublicKey(&key.PublicKey),
	) {
		return nil
	}

	return requestSignFunc(key, req)
}

// TODO: write docs, write tests.
func prepareSearchRequest(req transport.SearchInfo) serviceRequest {
	return &object.SearchRequest{
		ContainerID:  req.GetCID(),
		Query:        req.GetQuery(),
		QueryVersion: 1,
	}
}

func prepareGetRequest(req transport.GetInfo) serviceRequest {
	return &object.GetRequest{
		Address: req.GetAddress(),
	}
}

func prepareHeadRequest(req transport.HeadInfo) serviceRequest {
	return &object.HeadRequest{
		Address:     req.GetAddress(),
		FullHeaders: req.GetFullHeaders(),
	}
}

func preparePutRequest(req transport.PutInfo) serviceRequest {
	obj := req.GetHead()
	chunks := splitBytes(obj.Payload, maxGetPayloadSize)

	// copy object to save payload of initial object unchanged
	nObj := new(Object)
	*nObj = *obj
	nObj.Payload = nil

	res := &putRequestSequence{
		PutRequest: object.MakePutRequestHeader(nObj),
		chunks:     make([]*object.PutRequest, 0, len(chunks)),
	}

	// TODO: think about chunk messages signing
	for i := range chunks {
		res.chunks = append(res.chunks, object.MakePutRequestChunk(chunks[i]))
	}

	return res
}

func prepareRangeHashRequest(req transport.RangeHashInfo) serviceRequest {
	return &object.GetRangeHashRequest{
		Address: req.GetAddress(),
		Ranges:  req.GetRanges(),
		Salt:    req.GetSalt(),
	}
}

func prepareRangeRequest(req transport.RangeInfo) serviceRequest {
	return &GetRangeRequest{
		Address: req.GetAddress(),
		Range:   req.GetRange(),
	}
}

// TODO: write docs, write tests.
func (s *coreRequestSender) defaultTimeout(req transport.MetaInfo) time.Duration {
	switch req.Type() {
	case object.RequestSearch:
		return s.searchTimeout
	case object.RequestPut:
		return s.putTimeout
	case object.RequestGet:
		return s.getTimeout
	case object.RequestHead:
		return s.headTimeout
	case object.RequestRangeHash:
		return s.rangeHashTimeout
	}

	return minRemoteRequestTimeout
}

// TODO: write docs, write tests.
func (s *coreRequestSender) sendRequest(ctx context.Context, p sendParams) (interface{}, error) {
	var err error

	if p.node == nil {
		if p.node, err = s.addressStore.SelfAddr(); err != nil {
			return nil, err
		}
	}

	timeout := p.req.GetTimeout()
	if timeout <= 0 {
		timeout = s.defaultTimeout(p.req)
	}

	r, err := s.requestPrep.prepareRequest(p.req)
	if err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithTimeout(ctx, s.dialTimeout)

	c, err := s.remoteService.Remote(dialCtx, p.node)

	cancel()

	if err != nil {
		return nil, err
	}

	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	return p.handler.call(ctx, r, &clientInfo{
		sc:  c,
		key: s.addressStore.PublicKey(p.node),
	})
}

// NewMultiTransport is an object transport component's constructor.
func NewMultiTransport(p MultiTransportParams) (transport.ObjectTransport, error) {
	switch {
	case p.RemoteService == nil:
		return nil, errEmptyGRPC
	case p.AddressStore == nil:
		return nil, errEmptyAddress
	case p.Logger == nil:
		return nil, errEmptyLogger
	case p.EpochReceiver == nil:
		return nil, errEmptyEpochReceiver
	case p.Key == nil:
		return nil, errEmptyPrivateKey
	case p.PrivateTokenStore == nil:
		return nil, errEmptyTokenStore
	}

	if p.PutTimeout <= 0 {
		p.PutTimeout = minRemoteRequestTimeout
	}

	if p.GetTimeout <= 0 {
		p.GetTimeout = minRemoteRequestTimeout
	}

	if p.HeadTimeout <= 0 {
		p.HeadTimeout = minRemoteRequestTimeout
	}

	if p.SearchTimeout <= 0 {
		p.SearchTimeout = minRemoteRequestTimeout
	}

	if p.RangeHashTimeout <= 0 {
		p.RangeHashTimeout = minRemoteRequestTimeout
	}

	if p.DialTimeout <= 0 {
		p.DialTimeout = minDialTimeout
	}

	return &transportComponent{
		reqSender: &coreRequestSender{
			requestPrep: &coreRequestPreparer{
				epochRecv:   p.EpochReceiver,
				key:         p.Key,
				signingFunc: requestSignFunc,

				privateTokenStore: p.PrivateTokenStore,
			},
			addressStore:     p.AddressStore,
			remoteService:    p.RemoteService,
			putTimeout:       p.PutTimeout,
			getTimeout:       p.GetTimeout,
			searchTimeout:    p.SearchTimeout,
			headTimeout:      p.HeadTimeout,
			rangeHashTimeout: p.RangeHashTimeout,
			dialTimeout:      p.DialTimeout,
		},
		resTracker:      &idleResultTracker{},
		getCaller:       &getCaller{},
		putCaller:       &putCaller{},
		headCaller:      &headCaller{},
		rangeCaller:     &rangeCaller{},
		rangeHashCaller: &rangeHashCaller{},
		searchCaller:    &searchCaller{},
	}, nil
}
