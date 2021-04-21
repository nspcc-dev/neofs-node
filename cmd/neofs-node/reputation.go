package main

import (
	"context"

	v2reputation "github.com/nspcc-dev/neofs-api-go/v2/reputation"
	v2reputationgrpc "github.com/nspcc-dev/neofs-api-go/v2/reputation/grpc"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/intermediate"
	localreputation "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/local"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	grpcreputation "github.com/nspcc-dev/neofs-node/pkg/network/transport/reputation/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	reputationrouter "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common/router"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/daughters"
	trustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/managers"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	reputationrpc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/rpc"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func initReputationService(c *cfg) {
	// consider sharing this between application components
	nmSrc := newCachedNetmapStorage(c.cfgNetmap.state, c.cfgNetmap.wrapper)

	// storing calculated trusts as a daughter
	c.cfgReputation.localTrustStorage = truststorage.New(truststorage.Prm{})

	// storing received trusts as a manager
	daughterStorage := &intermediate.DaughterStorage{
		Log:     c.log,
		Storage: daughters.New(daughters.Prm{}),
	}

	trustStorage := &localreputation.TrustStorage{
		Log:      c.log,
		Storage:  c.cfgReputation.localTrustStorage,
		NmSrc:    nmSrc,
		LocalKey: crypto.MarshalPublicKey(&c.key.PublicKey),
	}

	managerBuilder := localreputation.NewManagerBuilder(
		localreputation.ManagersPrm{
			NetMapSource: nmSrc,
		},
		localreputation.WithLogger(c.log),
	)

	routeBuilder := managers.New(managers.Prm{
		ManagerBuilder: managerBuilder,
	})

	remoteLocalTrustProvider := localreputation.NewRemoteTrustProvider(
		localreputation.RemoteProviderPrm{
			LocalAddrSrc:    c,
			DeadEndProvider: daughterStorage,
			ClientCache:     cache.NewSDKClientCache(),
			Key:             c.key,
		},
	)

	router := reputationrouter.New(
		reputationrouter.Prm{
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

	// initialize eigen trust block timer
	durationMeter := NewEigenTrustDuration(c.cfgNetmap.wrapper)

	newEigenTrustIterTimer(c, durationMeter, func() {
		c.log.Debug("todo: start next EigenTrust iteration round")
	})

	addNewEpochAsyncNotificationHandler(
		c,
		func(e event.Event) {
			durationMeter.Update() // recalculate duration of one iteration round

			err := c.cfgMorph.eigenTrustTimer.Reset() // start iteration rounds again
			if err != nil {
				c.log.Warn("can't reset block timer to start eigen trust calculations again",
					zap.String("error", err.Error()))
			}
		},
	)
}

type reputationServer struct {
	*cfg
	log          *logger.Logger
	router       reputationcommon.WriterProvider
	routeBuilder reputationrouter.Builder
}

func (s *reputationServer) SendLocalTrust(ctx context.Context, req *v2reputation.SendLocalTrustRequest) (*v2reputation.SendLocalTrustResponse, error) {
	var passedRoute []reputationrouter.ServerInfo

	for hdr := req.GetVerificationHeader(); hdr != nil; hdr = hdr.GetOrigin() {
		passedRoute = append(passedRoute, &localreputation.OnlyKeyRemoteServerInfo{
			Key: hdr.GetBodySignature().GetKey(),
		})
	}

	for left, right := 0, len(passedRoute)-1; left < right; left, right = left+1, right-1 {
		passedRoute[left], passedRoute[right] = passedRoute[right], passedRoute[left]
	}

	passedRoute = append(passedRoute, s)

	body := req.GetBody()

	eCtx := &localreputation.EpochContext{
		Context: ctx,
		E:       body.GetEpoch(),
	}

	w, err := s.router.InitWriter(reputationrouter.NewRouteContext(eCtx, passedRoute))
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
	passedRoute []reputationrouter.ServerInfo, w reputationcommon.Writer) error {
	err := reputationrouter.CheckRoute(s.routeBuilder, epoch, t, passedRoute)
	if err != nil {
		return errors.Wrap(err, "wrong route of reputation trust value")
	}

	return w.Write(&localreputation.EpochContext{E: epoch}, t)
}
