package main

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/common"
	intermediatereputation "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/intermediate"
	localreputation "github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/local"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/reputation/ticker"
	repClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
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
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	protoreputation "github.com/nspcc-dev/neofs-sdk-go/proto/reputation"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	protostatus "github.com/nspcc-dev/neofs-sdk-go/proto/status"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"go.uber.org/zap"
)

func initReputationService(c *cfg) {
	wrap, err := repClient.NewFromMorph(c.cfgMorph.client, c.shared.basics.reputationSH, 0)
	fatalOnErr(err)

	localKey := c.key.PublicKey().Bytes()

	nmSrc := c.netMapSource

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

	localTrustLogger := c.log.With(zap.String("trust_type", "local"))
	intermediateTrustLogger := c.log.With(zap.String("trust_type", "intermediate"))

	localTrustStorage := &localreputation.TrustStorage{
		Log:      localTrustLogger,
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
			Log:            localTrustLogger,
		},
	)

	intermediateRouteBuilder := intermediateroutes.New(
		intermediateroutes.Prm{
			ManagerBuilder: managerBuilder,
			Log:            intermediateTrustLogger,
		},
	)

	remoteLocalTrustProvider := common.NewRemoteTrustProvider(
		common.RemoteProviderPrm{
			NetmapKeys:      c,
			DeadEndProvider: daughterStorageWriterProvider,
			ClientCache:     c.bgClientCache,
			WriterProvider: localreputation.NewRemoteProvider(
				localreputation.RemoteProviderPrm{
					Key: &c.key.PrivateKey,
					Log: localTrustLogger,
				},
			),
			Log: localTrustLogger,
		},
	)

	remoteIntermediateTrustProvider := common.NewRemoteTrustProvider(
		common.RemoteProviderPrm{
			NetmapKeys:      c,
			DeadEndProvider: consumerStorageWriterProvider,
			ClientCache:     c.bgClientCache,
			WriterProvider: intermediatereputation.NewRemoteProvider(
				intermediatereputation.RemoteProviderPrm{
					Key: &c.key.PrivateKey,
					Log: intermediateTrustLogger,
				},
			),
			Log: intermediateTrustLogger,
		},
	)

	localTrustRouter := reputationrouter.New(
		reputationrouter.Prm{
			LocalServerInfo:      c,
			RemoteWriterProvider: remoteLocalTrustProvider,
			Builder:              localRouteBuilder,
		},
		reputationrouter.WithLogger(localTrustLogger))

	intermediateTrustRouter := reputationrouter.New(
		reputationrouter.Prm{
			LocalServerInfo:      c,
			RemoteWriterProvider: remoteIntermediateTrustProvider,
			Builder:              intermediateRouteBuilder,
		},
		reputationrouter.WithLogger(intermediateTrustLogger),
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
		eigentrustctrl.WithLogger(c.log),
	)

	c.cfgReputation.localTrustCtrl = localtrustcontroller.New(
		localtrustcontroller.Prm{
			LocalTrustSource: localTrustStorage,
			LocalTrustTarget: localTrustRouter,
		},
		localtrustcontroller.WithLogger(c.log),
	)

	addNewEpochAsyncNotificationHandler(
		c,
		func(ev event.Event) {
			c.log.Debug("start reporting reputation on new epoch event")

			var reportPrm localtrustcontroller.ReportPrm

			// report collected values from previous epoch
			reportPrm.SetEpoch(ev.(netmap.NewEpoch).EpochNumber() - 1)

			c.cfgReputation.localTrustCtrl.Report(reportPrm)
		},
	)

	server := &reputationServer{
		cfg:                c,
		log:                c.log,
		localRouter:        localTrustRouter,
		intermediateRouter: intermediateTrustRouter,
		routeBuilder:       localRouteBuilder,
	}

	for _, srv := range c.cfgGRPC.servers {
		protoreputation.RegisterReputationServiceServer(srv, server)
	}

	// initialize eigen trust block timer
	newEigenTrustIterTimer(c)

	addNewEpochAsyncNotificationHandler(
		c,
		func(e event.Event) {
			epoch := e.(netmap.NewEpoch).EpochNumber()

			log := c.log.With(zap.Uint64("epoch", epoch))

			duration, err := c.cfgNetmap.wrapper.EpochDuration()
			if err != nil {
				log.Debug("could not fetch epoch duration", zap.Error(err))
				return
			}
			c.networkState.updateEpochDuration(duration)

			iterations, err := c.cfgNetmap.wrapper.EigenTrustIterations()
			if err != nil {
				log.Debug("could not fetch iteration number", zap.Error(err))
				return
			}

			epochTimer, err := ticker.NewIterationsTicker(duration, iterations, func() {
				eigenTrustController.Continue(
					eigentrustctrl.ContinuePrm{
						Epoch: epoch - 1,
					},
				)
			})
			if err != nil {
				log.Debug("could not create fixed epoch timer", zap.Error(err))
				return
			}

			c.cfgMorph.eigenTrustTicker.addEpochTimer(epoch, epochTimer)
		},
	)
}

type reputationServer struct {
	*cfg
	log                *zap.Logger
	localRouter        reputationcommon.WriterProvider
	intermediateRouter reputationcommon.WriterProvider
	routeBuilder       reputationrouter.Builder
}

func (s *reputationServer) makeResponseMetaHeader(st *protostatus.Status) *protosession.ResponseMetaHeader {
	return &protosession.ResponseMetaHeader{
		Version: version.Current().ProtoMessage(),
		Epoch:   s.networkState.CurrentEpoch(),
		Status:  st,
	}
}

func (s *reputationServer) makeLocalResponse(err error) (*protoreputation.AnnounceLocalTrustResponse, error) {
	resp := &protoreputation.AnnounceLocalTrustResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(&s.key.PrivateKey, resp)
	return resp, nil
}

func (s *reputationServer) AnnounceLocalTrust(ctx context.Context, req *protoreputation.AnnounceLocalTrustRequest) (*protoreputation.AnnounceLocalTrustResponse, error) {
	if err := neofscrypto.VerifyRequestWithBuffer(req, nil); err != nil {
		return s.makeLocalResponse(util.ToRequestSignatureVerificationError(err))
	}

	passedRoute := reverseRoute(req.GetVerifyHeader())
	passedRoute = append(passedRoute, s)

	body := req.GetBody()

	eCtx := &common.EpochContext{
		Context: ctx,
		E:       body.GetEpoch(),
	}

	w, err := s.localRouter.InitWriter(reputationrouter.NewRouteContext(eCtx, passedRoute))
	if err != nil {
		return s.makeLocalResponse(fmt.Errorf("could not initialize local trust writer: %w", err))
	}

	for _, trust := range body.GetTrusts() {
		err = s.processLocalTrust(body.GetEpoch(), apiToLocalTrust(trust, passedRoute[0].PublicKey()), passedRoute, w)
		if err != nil {
			return s.makeLocalResponse(fmt.Errorf("could not write one of local trusts: %w", err))
		}
	}

	return s.makeLocalResponse(util.StatusOKErr)
}

func (s *reputationServer) makeIntermediateResponse(err error) (*protoreputation.AnnounceIntermediateResultResponse, error) {
	resp := &protoreputation.AnnounceIntermediateResultResponse{
		MetaHeader: s.makeResponseMetaHeader(util.ToStatus(err)),
	}
	resp.VerifyHeader = util.SignResponse(&s.key.PrivateKey, resp)
	return resp, nil
}

func (s *reputationServer) AnnounceIntermediateResult(ctx context.Context, req *protoreputation.AnnounceIntermediateResultRequest) (*protoreputation.AnnounceIntermediateResultResponse, error) {
	if err := neofscrypto.VerifyRequestWithBuffer(req, nil); err != nil {
		return s.makeIntermediateResponse(util.ToRequestSignatureVerificationError(err))
	}

	passedRoute := reverseRoute(req.GetVerifyHeader())
	passedRoute = append(passedRoute, s)

	body := req.GetBody()

	eiCtx := eigentrust.NewIterContext(ctx, body.GetEpoch(), body.GetIteration())

	w, err := s.intermediateRouter.InitWriter(reputationrouter.NewRouteContext(eiCtx, passedRoute))
	if err != nil {
		return s.makeIntermediateResponse(fmt.Errorf("could not initialize trust writer: %w", err))
	}

	v2Trust := body.GetTrust()

	trust := apiToLocalTrust(v2Trust.GetTrust(), v2Trust.GetTrustingPeer().GetPublicKey())

	err = w.Write(trust)
	if err != nil {
		return s.makeIntermediateResponse(fmt.Errorf("could not write trust: %w", err))
	}

	return s.makeIntermediateResponse(util.StatusOKErr)
}

func (s *reputationServer) processLocalTrust(epoch uint64, t reputation.Trust,
	passedRoute []reputationcommon.ServerInfo, w reputationcommon.Writer) error {
	err := reputationrouter.CheckRoute(s.routeBuilder, epoch, t, passedRoute)
	if err != nil {
		return fmt.Errorf("wrong route of reputation trust value: %w", err)
	}

	return w.Write(t)
}

// apiToLocalTrust converts protoreputation.Trust to local reputation.Trust,
// adding trustingPeer.
func apiToLocalTrust(t *protoreputation.Trust, trustingPeer []byte) reputation.Trust {
	var trusted, trusting apireputation.PeerID
	trusted.SetPublicKey(t.GetPeer().GetPublicKey())
	trusting.SetPublicKey(trustingPeer)

	localTrust := reputation.Trust{}

	localTrust.SetValue(reputation.TrustValueFromFloat64(t.GetValue()))
	localTrust.SetPeer(trusted)
	localTrust.SetTrustingPeer(trusting)

	return localTrust
}

func reverseRoute(hdr *protosession.RequestVerificationHeader) (passedRoute []reputationcommon.ServerInfo) {
	for hdr != nil {
		passedRoute = append(passedRoute, &common.OnlyKeyRemoteServerInfo{
			Key: hdr.GetBodySignature().GetKey(),
		})

		hdr = hdr.GetOrigin()
	}

	return
}
