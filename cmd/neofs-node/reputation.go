package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/hex"

	"github.com/nspcc-dev/hrw"
	apiClient "github.com/nspcc-dev/neofs-api-go/pkg/client"
	apiNetmap "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	reputationapi "github.com/nspcc-dev/neofs-api-go/pkg/reputation"
	v2reputation "github.com/nspcc-dev/neofs-api-go/v2/reputation"
	v2reputationgrpc "github.com/nspcc-dev/neofs-api-go/v2/reputation/grpc"
	crypto "github.com/nspcc-dev/neofs-crypto"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	grpcreputation "github.com/nspcc-dev/neofs-node/pkg/network/transport/reputation/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	reputationroute "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/route"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/route/managers"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	reputationrpc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/rpc"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type localTrustStorage struct {
	log *logger.Logger

	storage *truststorage.Storage

	nmSrc netmapcore.Source

	localKey []byte
}

type localTrustIterator struct {
	ctx reputationcommon.Context

	storage *localTrustStorage

	epochStorage *truststorage.EpochTrustValueStorage
}

type managerBuilder struct {
	log   *logger.Logger
	nmSrc netmapcore.Source
}

type remoteLocalTrustProvider struct {
	localAddrSrc    network.LocalAddressSource
	deadEndProvider reputationcommon.WriterProvider
	key             *ecdsa.PrivateKey

	clientCache interface {
		Get(string) (apiClient.Client, error)
	}
}

type nopReputationWriter struct{}

func (nopReputationWriter) Write(reputation.Trust) error {
	return nil
}

func (nopReputationWriter) Close() error {
	return nil
}

type remoteLocalTrustWriter struct {
	ctx    reputationcommon.Context
	client apiClient.Client
	key    *ecdsa.PrivateKey

	buf []*reputationapi.Trust
}

func (rtp *remoteLocalTrustWriter) Write(t reputation.Trust) error {
	apiTrust := reputationapi.NewTrust()

	apiPeer := reputationapi.NewPeerID()
	apiPeer.SetPublicKey(t.Peer())

	apiTrust.SetValue(t.Value().Float64())
	apiTrust.SetPeer(apiPeer)

	rtp.buf = append(rtp.buf, apiTrust)

	return nil
}

func (rtp *remoteLocalTrustWriter) Close() error {
	prm := apiClient.SendLocalTrustPrm{}

	prm.SetEpoch(rtp.ctx.Epoch())
	prm.SetTrusts(rtp.buf)

	_, err := rtp.client.SendLocalTrust(
		rtp.ctx,
		prm,
		apiClient.WithKey(rtp.key),
	)

	return err
}

type remoteLocalTrustWriterProvider struct {
	client apiClient.Client
	key    *ecdsa.PrivateKey
}

type localTrustLogger struct {
	ctx reputationcommon.Context

	log *logger.Logger
}

func (l *localTrustLogger) Write(t reputation.Trust) error {
	l.log.Info("received local trust",
		zap.Uint64("epoch", l.ctx.Epoch()),
		zap.String("peer", hex.EncodeToString(t.Peer().Bytes())),
		zap.Stringer("value", t.Value()),
	)

	return nil
}

func (*localTrustLogger) Close() error {
	return nil
}

func (rtwp *remoteLocalTrustWriterProvider) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	return &remoteLocalTrustWriter{
		ctx:    ctx,
		client: rtwp.client,
		key:    rtwp.key,
	}, nil
}

func (rtp *remoteLocalTrustProvider) InitRemote(srv reputationroute.ServerInfo) (reputationcommon.WriterProvider, error) {
	if srv == nil {
		return rtp.deadEndProvider, nil
	}

	addr := srv.Address()

	if rtp.localAddrSrc.LocalAddress().String() == srv.Address() {
		// if local => return no-op writer
		return trustcontroller.SimpleWriterProvider(new(nopReputationWriter)), nil
	}

	ipAddr, err := network.IPAddrFromMultiaddr(addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert address to IP format")
	}

	c, err := rtp.clientCache.Get(ipAddr)
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize API client")
	}

	return &remoteLocalTrustWriterProvider{
		client: c,
		key:    rtp.key,
	}, nil
}

// BuildManagers sorts nodes in NetMap with HRW algorithms and
// takes the next node after the current one as the only manager.
func (mb *managerBuilder) BuildManagers(epoch uint64, p reputation.PeerID) ([]reputationroute.ServerInfo, error) {
	nm, err := mb.nmSrc.GetNetMapByEpoch(epoch)
	if err != nil {
		return nil, err
	}

	// make a copy to keep order consistency of the origin netmap after sorting
	nodes := make([]*apiNetmap.Node, len(nm.Nodes))

	copy(nodes, nm.Nodes)

	hrw.SortSliceByValue(nodes, epoch)

	for i := range nodes {
		if bytes.Equal(nodes[i].PublicKey(), p.Bytes()) {
			managerIndex := i + 1

			if managerIndex == len(nodes) {
				managerIndex = 0
			}

			return []reputationroute.ServerInfo{nodes[managerIndex]}, nil
		}
	}

	return nil, nil
}

func (s *localTrustStorage) InitIterator(ctx reputationcommon.Context) (trustcontroller.Iterator, error) {
	epochStorage, err := s.storage.DataForEpoch(ctx.Epoch())
	if err != nil && !errors.Is(err, truststorage.ErrNoPositiveTrust) {
		return nil, err
	}

	return &localTrustIterator{
		ctx:          ctx,
		storage:      s,
		epochStorage: epochStorage,
	}, nil
}

func (s *localTrustStorage) InitWriter(ctx reputationcommon.Context) (reputationcommon.Writer, error) {
	return &localTrustLogger{
		ctx: ctx,
		log: s.log,
	}, nil
}

func (it *localTrustIterator) Iterate(h reputation.TrustHandler) error {
	if it.epochStorage != nil {
		err := it.epochStorage.Iterate(h)
		if !errors.Is(err, truststorage.ErrNoPositiveTrust) {
			return err
		}
	}

	nm, err := it.storage.nmSrc.GetNetMapByEpoch(it.ctx.Epoch())
	if err != nil {
		return err
	}

	// find out if local node is presented in netmap
	localIndex := -1

	for i := range nm.Nodes {
		if bytes.Equal(nm.Nodes[i].PublicKey(), it.storage.localKey) {
			localIndex = i
		}
	}

	ln := len(nm.Nodes)
	if localIndex >= 0 && ln > 0 {
		ln--
	}

	// calculate Pj http://ilpubs.stanford.edu:8090/562/1/2002-56.pdf Chapter 4.5.
	p := reputation.TrustOne.Div(reputation.TrustValueFromInt(ln))

	for i := range nm.Nodes {
		if i == localIndex {
			continue
		}

		trust := reputation.Trust{}
		trust.SetPeer(reputation.PeerIDFromBytes(nm.Nodes[i].PublicKey()))
		trust.SetValue(p)

		if err := h(trust); err != nil {
			return err
		}
	}

	return nil
}

func initReputationService(c *cfg) {
	// consider sharing this between application components
	nmSrc := newCachedNetmapStorage(c.cfgNetmap.state, c.cfgNetmap.wrapper)

	c.cfgReputation.localTrustStorage = truststorage.New(truststorage.Prm{})

	trustStorage := &localTrustStorage{
		log:      c.log,
		storage:  c.cfgReputation.localTrustStorage,
		nmSrc:    nmSrc,
		localKey: crypto.MarshalPublicKey(&c.key.PublicKey),
	}

	managerBuilder := &managerBuilder{
		log:   c.log,
		nmSrc: nmSrc,
	}

	routeBuilder := managers.New(managers.Prm{
		ManagerBuilder: managerBuilder,
	})

	remoteLocalTrustProvider := &remoteLocalTrustProvider{
		localAddrSrc:    c,
		deadEndProvider: trustStorage,
		clientCache:     cache.NewSDKClientCache(),
		key:             c.key,
	}

	router := reputationroute.New(
		reputationroute.Prm{
			LocalServerInfo:      c,
			RemoteWriterProvider: remoteLocalTrustProvider,
			Builder:              routeBuilder,
		})

	c.cfgReputation.localTrustCtrl = trustcontroller.New(trustcontroller.Prm{
		LocalTrustSource: trustStorage,
		LocalTrustTarget: router,
	})

	addNewEpochAsyncNotificationHandler(
		c,
		func(ev event.Event) {
			var reportPrm trustcontroller.ReportPrm

			// report collected values from previous epoch
			reportPrm.SetEpoch(ev.(netmap.NewEpoch).EpochNumber() - 1)

			c.cfgReputation.localTrustCtrl.Report(reportPrm)
		},
	)

	v2reputationgrpc.RegisterReputationServiceServer(c.cfgGRPC.server,
		grpcreputation.New(
			reputationrpc.NewSignService(
				c.key,
				reputationrpc.NewResponseService(
					&reputationServer{
						cfg:          c,
						log:          c.log,
						router:       router,
						routeBuilder: routeBuilder,
					},
					c.respSvc,
				),
			),
		),
	)
}

type reputationServer struct {
	*cfg
	log          *logger.Logger
	router       reputationcommon.WriterProvider
	routeBuilder reputationroute.Builder
}

type epochContext struct {
	context.Context
	epoch uint64
}

func (ctx *epochContext) Epoch() uint64 {
	return ctx.epoch
}

type reputationOnlyKeyRemoteServerInfo struct {
	key []byte
}

func (i *reputationOnlyKeyRemoteServerInfo) PublicKey() []byte {
	return i.key
}

func (*reputationOnlyKeyRemoteServerInfo) Address() string {
	return ""
}

func (s *reputationServer) SendLocalTrust(ctx context.Context, req *v2reputation.SendLocalTrustRequest) (*v2reputation.SendLocalTrustResponse, error) {
	var passedRoute []reputationroute.ServerInfo

	for hdr := req.GetVerificationHeader(); hdr != nil; hdr = hdr.GetOrigin() {
		passedRoute = append(passedRoute, &reputationOnlyKeyRemoteServerInfo{
			key: hdr.GetBodySignature().GetKey(),
		})
	}

	for left, right := 0, len(passedRoute)-1; left < right; left, right = left+1, right-1 {
		passedRoute[left], passedRoute[right] = passedRoute[right], passedRoute[left]
	}

	passedRoute = append(passedRoute, s)

	body := req.GetBody()

	eCtx := &epochContext{
		Context: ctx,
		epoch:   body.GetEpoch(),
	}

	w, err := s.router.InitWriter(reputationroute.NewRouteContext(eCtx, passedRoute))
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize local trust writer")
	}

	for _, trust := range body.GetTrusts() {
		err = s.processTrust(body.GetEpoch(), apiToLocalTrust(trust, passedRoute[0].PublicKey()), passedRoute, w)
		if err != nil {
			return nil, errors.Wrap(err, "could not write one of trusts")
		}
	}

	resp := new(v2reputation.SendLocalTrustResponse)
	resp.SetBody(new(v2reputation.SendLocalTrustResponseBody))

	return resp, nil
}

func (s *reputationServer) SendIntermediateResult(_ context.Context, req *v2reputation.SendIntermediateResultRequest) (*v2reputation.SendIntermediateResultResponse, error) {
	resp := new(v2reputation.SendIntermediateResultResponse)

	// todo: implement me

	return resp, nil
}

// apiToLocalTrust converts v2 Trust to local reputation.Trust, adding trustingPeer.
func apiToLocalTrust(t *v2reputation.Trust, trustingPeer []byte) reputation.Trust {
	localTrust := reputation.Trust{}

	localTrust.SetValue(reputation.TrustValueFromFloat64(t.GetValue()))
	localTrust.SetPeer(reputation.PeerIDFromBytes(t.GetPeer().GetValue()))
	localTrust.SetTrustingPeer(reputation.PeerIDFromBytes(trustingPeer))

	return localTrust
}

func (s *reputationServer) processTrust(epoch uint64, t reputation.Trust,
	passedRoute []reputationroute.ServerInfo, w reputationcommon.Writer) error {
	err := reputationroute.CheckRoute(s.routeBuilder, epoch, t, passedRoute)
	if err != nil {
		return errors.Wrap(err, "wrong route of reputation trust value")
	}

	return w.Write(t)
}
