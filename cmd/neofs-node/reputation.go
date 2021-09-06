package main

import (
	"context"
	"fmt"

	v2reputation "github.com/nspcc-dev/neofs-api-go/v2/reputation"
	v2reputationgrpc "github.com/nspcc-dev/neofs-api-go/v2/reputation/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	intermediatereputation "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/intermediate"
	localreputation "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/local"
	rtpwrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
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
	"go.uber.org/zap"
)

func initReputationService(c *cfg) {
	wrap, err := rtpwrapper.NewFromMorph(c.cfgMorph.client, c.cfgReputation.scriptHash, 0)
	fatalOnErr(err)

	localKey := c.key.PublicKey().Bytes()

	// consider sharing this between application components
	nmSrc := newCachedNetmapStorage(c.cfgNetmap.state, c.cfgNetmap.wrapper)

	// storing calculated trusts as a daughter
	c.cfgReputation.localTrustStorage = truststorage.New(
		truststorage.Prm{},
	)

	daughterStorage := daughters.New(daughters.Prm{})
	consumerStorage := consumerstorage.New(consumerstorage.Prm{})

	// storing received daughter(of current node) trusts as a manager
	daughterStorageWriterProvider := &intermediatereputation.DaughterStorageWriterProvider{
		Log:     c.log,
		Storage: daughterStorage,
	}

	consumerStorageWriterProvider := &intermediatereputation.ConsumerStorageWriterProvider{
		Log:     c.log,
		Storage: consumerStorage,
	}

	localTrustStorage := &localreputation.TrustStorage{
		Log:      c.log,
		Storage:  c.cfgReputation.localTrustStorage,
		NmSrc:    nmSrc,
		LocalKey: localKey,
	}

	managerBuilder := reputationcommon.NewManagerBuilder(
		reputationcommon.ManagersPrm{
			NetMapSource: nmSrc,
		},
		reputationcommon.WithLogger(c.log),
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

	remoteLocalTrustProvider := common.NewRemoteTrustProvider(
		common.RemoteProviderPrm{
			NetmapKeys:      c,
			DeadEndProvider: daughterStorageWriterProvider,
			ClientCache:     c.clientCache,
			WriterProvider: localreputation.NewRemoteProvider(
				localreputation.RemoteProviderPrm{
					Key: &c.key.PrivateKey,
				},
			),
		},
	)

	remoteIntermediateTrustProvider := common.NewRemoteTrustProvider(
		common.RemoteProviderPrm{
			NetmapKeys:      c,
			DeadEndProvider: consumerStorageWriterProvider,
			ClientCache:     c.clientCache,
			WriterProvider: intermediatereputation.NewRemoteProvider(
				intermediatereputation.RemoteProviderPrm{
					Key: &c.key.PrivateKey,
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
			FinalResultTarget: intermediatereputation.NewFinalWriterProvider(
				intermediatereputation.FinalWriterProviderPrm{
					PrivatKey: &c.key.PrivateKey,
					PubKey:    localKey,
					Client:    wrap,
				},
				intermediatereputation.FinalWriterWithLogger(c.log),
			),
			DaughterTrustSource: &intermediatereputation.DaughterTrustIteratorProvider{
				DaughterStorage: daughterStorage,
				ConsumerStorage: consumerStorage,
			},
		},
		eigentrustcalc.WithLogger(c.log),
	)

	eigenTrustController := eigentrustctrl.New(
		eigentrustctrl.Prm{
			DaughtersTrustCalculator: &intermediatereputation.DaughtersTrustCalculator{
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

	server := grpcreputation.New(
		reputationrpc.NewSignService(
			&c.key.PrivateKey,
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
	)

	for _, srv := range c.cfgGRPC.servers {
		v2reputationgrpc.RegisterReputationServiceServer(srv, server)
	}

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
		return nil, fmt.Errorf("could not initialize local trust writer: %w", err)
	}

	for _, trust := range body.GetTrusts() {
		err = s.processLocalTrust(body.GetEpoch(), apiToLocalTrust(trust, passedRoute[0].PublicKey()), passedRoute, w)
		if err != nil {
			return nil, fmt.Errorf("could not write one of local trusts: %w", err)
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
		return nil, fmt.Errorf("could not initialize intermediate trust writer: %w", err)
	}

	v2Trust := body.GetTrust()

	trust := apiToLocalTrust(v2Trust.GetTrust(), v2Trust.GetTrustingPeer().GetPublicKey())

	err = w.Write(trust)
	if err != nil {
		return nil, fmt.Errorf("could not write intermediate trust: %w", err)
	}

	resp := new(v2reputation.AnnounceIntermediateResultResponse)
	resp.SetBody(new(v2reputation.AnnounceIntermediateResultResponseBody))

	return resp, nil
}

func (s *reputationServer) processLocalTrust(epoch uint64, t reputation.Trust,
	passedRoute []reputationcommon.ServerInfo, w reputationcommon.Writer) error {
	err := reputationrouter.CheckRoute(s.routeBuilder, epoch, t, passedRoute)
	if err != nil {
		return fmt.Errorf("wrong route of reputation trust value: %w", err)
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
