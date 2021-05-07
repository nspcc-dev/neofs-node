package main

import (
	"context"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	v2reputation "github.com/nspcc-dev/neofs-api-go/v2/reputation"
	v2reputationgrpc "github.com/nspcc-dev/neofs-api-go/v2/reputation/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/intermediate"
	intermediatereputation "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/intermediate"
	localreputation "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/local"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	rptclient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
	rtpwrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	grpcreputation "github.com/nspcc-dev/neofs-node/pkg/network/transport/reputation/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	reputationrouter "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common/router"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	eigentrustcalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	eigentrustctrl "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/controller"
	intermediateroutes "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/routes"
	consumerstorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/consumers"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/storage/daughters"
	localtrustcontroller "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/controller"
	localroutes "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/routes"
	truststorage "github.com/nspcc-dev/neofs-node/pkg/services/reputation/local/storage"
	reputationrpc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/rpc"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func initReputationService(c *cfg) {
	staticClient, err := client.NewStatic(
		c.cfgMorph.client,
		c.cfgReputation.scriptHash,
		fixedn.Fixed8(0),
	)
	fatalOnErr(err)

	rptClient, err := rptclient.New(staticClient)
	fatalOnErr(err)

	wrap := rtpwrapper.WrapClient(rptClient)
	fatalOnErr(err)

	localKey := crypto.MarshalPublicKey(&c.key.PublicKey)

	// consider sharing this between application components
	nmSrc := newCachedNetmapStorage(c.cfgNetmap.state, c.cfgNetmap.wrapper)

	// storing calculated trusts as a daughter
	c.cfgReputation.localTrustStorage = truststorage.New(
		truststorage.Prm{},
	)

	daughterStorage := daughters.New(daughters.Prm{})
	consumerStorage := consumerstorage.New(consumerstorage.Prm{})

	// storing received daughter(of current node) trusts as a manager
	daughterStorageWriterProvider := &intermediate.DaughterStorageWriterProvider{
		Log:     c.log,
		Storage: daughterStorage,
	}

	consumerStorageWriterProvider := &intermediate.ConsumerStorageWriterProvider{
		Log:     c.log,
		Storage: consumerStorage,
	}

	localTrustStorage := &localreputation.TrustStorage{
		Log:      c.log,
		Storage:  c.cfgReputation.localTrustStorage,
		NmSrc:    nmSrc,
		LocalKey: localKey,
	}

	managerBuilder := common.NewManagerBuilder(
		common.ManagersPrm{
			NetMapSource: nmSrc,
		},
		common.WithLogger(c.log),
	)

	localRouteBuilder := localroutes.New(
		localroutes.Prm{
			ManagerBuilder: managerBuilder,
		},
	)

	intermediateRouteBuilder := intermediateroutes.New(
		intermediateroutes.Prm{
			ManagerBuilder: managerBuilder,
		},
	)

	apiClientCache := cache.NewSDKClientCache()

	remoteLocalTrustProvider := common.NewRemoteTrustProvider(
		common.RemoteProviderPrm{
			LocalAddrSrc:    c,
			DeadEndProvider: daughterStorageWriterProvider,
			ClientCache:     apiClientCache,
			WriterProvider: localreputation.NewRemoteProvider(
				localreputation.RemoteProviderPrm{
					Key: c.key,
				},
			),
		},
	)

	remoteIntermediateTrustProvider := common.NewRemoteTrustProvider(
		common.RemoteProviderPrm{
			LocalAddrSrc:    c,
			DeadEndProvider: consumerStorageWriterProvider,
			ClientCache:     apiClientCache,
			WriterProvider: intermediatereputation.NewRemoteProvider(
				intermediatereputation.RemoteProviderPrm{
					Key: c.key,
				},
			),
		},
	)

	localTrustRouter := reputationrouter.New(
		reputationrouter.Prm{
			LocalServerInfo:      c,
			RemoteWriterProvider: remoteLocalTrustProvider,
			Builder:              localRouteBuilder,
		},
	)

	intermediateTrustRouter := reputationrouter.New(
		reputationrouter.Prm{
			LocalServerInfo:      c,
			RemoteWriterProvider: remoteIntermediateTrustProvider,
			Builder:              intermediateRouteBuilder,
		},
	)

	eigenTrustCalculator := eigentrustcalc.New(
		eigentrustcalc.Prm{
			AlphaProvider: c.cfgNetmap.wrapper,
			InitialTrustSource: intermediatereputation.InitialTrustSource{
				NetMap: nmSrc,
			},
			IntermediateValueTarget: intermediateTrustRouter,
			WorkerPool:              c.cfgReputation.workerPool,
			FinalResultTarget: intermediate.NewFinalWriterProvider(
				intermediate.FinalWriterProviderPrm{
					PrivatKey: c.key,
					PubKey:    localKey,
					Client:    wrap,
				},
				intermediate.FinalWriterWithLogger(c.log),
			),
			DaughterTrustSource: &intermediate.DaughterTrustIteratorProvider{
				DaughterStorage: daughterStorage,
				ConsumerStorage: consumerStorage,
			},
		},
		eigentrustcalc.WithLogger(c.log),
	)

	eigenTrustController := eigentrustctrl.New(
		eigentrustctrl.Prm{
			DaughtersTrustCalculator: &intermediate.DaughtersTrustCalculator{
				Calculator: eigenTrustCalculator,
			},
			IterationsProvider: c.cfgNetmap.wrapper,
			WorkerPool:         c.cfgReputation.workerPool,
		},
	)

	c.cfgReputation.localTrustCtrl = localtrustcontroller.New(
		localtrustcontroller.Prm{
			LocalTrustSource: localTrustStorage,
			LocalTrustTarget: localTrustRouter,
		},
	)

	addNewEpochAsyncNotificationHandler(
		c,
		func(ev event.Event) {
			var reportPrm localtrustcontroller.ReportPrm

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
						cfg:                c,
						log:                c.log,
						localRouter:        localTrustRouter,
						intermediateRouter: intermediateTrustRouter,
						routeBuilder:       localRouteBuilder,
					},
					c.respSvc,
				),
			),
		),
	)

	// initialize eigen trust block timer
	durationMeter := NewEigenTrustDuration(c.cfgNetmap.wrapper)

	newEigenTrustIterTimer(c, durationMeter, func() {
		epoch, err := c.cfgNetmap.wrapper.Epoch()
		if err != nil {
			c.log.Debug(
				"could not get current epoch",
				zap.String("error", err.Error()),
			)

			return
		}

		eigenTrustController.Continue(
			eigentrustctrl.ContinuePrm{
				Epoch: epoch - 1,
			},
		)
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
	log                *logger.Logger
	localRouter        reputationcommon.WriterProvider
	intermediateRouter reputationcommon.WriterProvider
	routeBuilder       reputationrouter.Builder
}

func (s *reputationServer) AnnounceLocalTrust(ctx context.Context, req *v2reputation.AnnounceLocalTrustRequest) (*v2reputation.AnnounceLocalTrustResponse, error) {
	passedRoute := reverseRoute(req.GetVerificationHeader())
	passedRoute = append(passedRoute, s)

	body := req.GetBody()

	eCtx := &common.EpochContext{
		Context: ctx,
		E:       body.GetEpoch(),
	}

	w, err := s.localRouter.InitWriter(reputationrouter.NewRouteContext(eCtx, passedRoute))
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize local trust writer")
	}

	for _, trust := range body.GetTrusts() {
		err = s.processLocalTrust(body.GetEpoch(), apiToLocalTrust(trust, passedRoute[0].PublicKey()), passedRoute, w)
		if err != nil {
			return nil, errors.Wrap(err, "could not write one of local trusts")
		}
	}

	resp := new(v2reputation.AnnounceLocalTrustResponse)
	resp.SetBody(new(v2reputation.AnnounceLocalTrustResponseBody))

	return resp, nil
}

func (s *reputationServer) AnnounceIntermediateResult(ctx context.Context, req *v2reputation.AnnounceIntermediateResultRequest) (*v2reputation.AnnounceIntermediateResultResponse, error) {
	passedRoute := reverseRoute(req.GetVerificationHeader())
	passedRoute = append(passedRoute, s)

	body := req.GetBody()

	eiCtx := eigentrust.NewIterContext(ctx, body.GetEpoch(), body.GetIteration())

	w, err := s.intermediateRouter.InitWriter(reputationrouter.NewRouteContext(eiCtx, passedRoute))
	if err != nil {
		return nil, errors.Wrap(err, "could not initialize intermediate trust writer")
	}

	v2Trust := body.GetTrust()

	trust := apiToLocalTrust(v2Trust.GetTrust(), v2Trust.GetTrustingPeer().GetPublicKey())

	err = w.Write(trust)
	if err != nil {
		return nil, errors.Wrap(err, "could not write intermediate trust")
	}

	resp := new(v2reputation.AnnounceIntermediateResultResponse)
	resp.SetBody(new(v2reputation.AnnounceIntermediateResultResponseBody))

	return resp, nil
}

func (s *reputationServer) processLocalTrust(epoch uint64, t reputation.Trust,
	passedRoute []reputationcommon.ServerInfo, w reputationcommon.Writer) error {
	err := reputationrouter.CheckRoute(s.routeBuilder, epoch, t, passedRoute)
	if err != nil {
		return errors.Wrap(err, "wrong route of reputation trust value")
	}

	return w.Write(t)
}

// apiToLocalTrust converts v2 Trust to local reputation.Trust, adding trustingPeer.
func apiToLocalTrust(t *v2reputation.Trust, trustingPeer []byte) reputation.Trust {
	localTrust := reputation.Trust{}

	localTrust.SetValue(reputation.TrustValueFromFloat64(t.GetValue()))
	localTrust.SetPeer(reputation.PeerIDFromBytes(t.GetPeer().GetPublicKey()))
	localTrust.SetTrustingPeer(reputation.PeerIDFromBytes(trustingPeer))

	return localTrust
}

func reverseRoute(hdr *session.RequestVerificationHeader) (passedRoute []reputationcommon.ServerInfo) {
	for hdr != nil {
		passedRoute = append(passedRoute, &common.OnlyKeyRemoteServerInfo{
			Key: hdr.GetBodySignature().GetKey(),
		})

		hdr = hdr.GetOrigin()
	}

	return
}
