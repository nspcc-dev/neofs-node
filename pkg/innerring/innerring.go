package innerring

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/balance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/container"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	nodevalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation"
	addrvalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/maddress"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	auditSettlement "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/audit"
	timerEvent "github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	balanceWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	cntWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	neofsWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs/wrapper"
	neofsidWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid/wrapper"
	nmWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	repWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"github.com/nspcc-dev/neofs-node/pkg/morph/timer"
	audittask "github.com/nspcc-dev/neofs-node/pkg/services/audit/taskmanager"
	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	controlsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	util2 "github.com/nspcc-dev/neofs-node/pkg/util"
	utilConfig "github.com/nspcc-dev/neofs-node/pkg/util/config"
	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	// Server is the inner ring application structure, that contains all event
	// processors, shared variables and event handlers.
	Server struct {
		log *zap.Logger

		// event producers
		morphListener   event.Listener
		mainnetListener event.Listener
		blockTimers     []*timer.BlockTimer
		epochTimer      *timer.BlockTimer

		// global state
		morphClient   *client.Client
		mainnetClient *client.Client
		epochCounter  atomic.Uint64
		statusIndex   *innerRingIndexer
		precision     precision.Fixed8Converter
		auditClient   *auditWrapper.ClientWrapper
		healthStatus  atomic.Value
		balanceClient *balanceWrapper.Wrapper
		netmapClient  *nmWrapper.Wrapper

		// notary configuration
		feeConfig        *config.FeeConfig
		mainNotaryConfig *notaryConfig
		sideNotaryConfig *notaryConfig

		// internal variables
		key                   *keys.PrivateKey
		pubKey                []byte
		contracts             *contracts
		predefinedValidators  keys.PublicKeys
		initialEpochTickDelta uint32

		// runtime processors
		netmapProcessor *netmap.Processor

		workers []func(context.Context)

		// Set of local resources that must be
		// initialized at the very beginning of
		// Server's work, (e.g. opening files).
		//
		// If any starter returns an error, Server's
		// starting fails immediately.
		starters []func() error

		// Set of local resources that must be
		// released at Server's work completion
		// (e.g closing files).
		//
		// Closer's wrong outcome shouldn't be critical.
		//
		// Errors are logged.
		closers []func() error

		// Set of component runners which
		// should report start errors
		// to the application.
		//
		// TODO: unify with workers.
		runners []func(chan<- error)
	}

	contracts struct {
		neofs      util.Uint160 // in mainnet
		netmap     util.Uint160 // in morph
		balance    util.Uint160 // in morph
		container  util.Uint160 // in morph
		audit      util.Uint160 // in morph
		proxy      util.Uint160 // in morph
		processing util.Uint160 // in mainnet
		reputation util.Uint160 // in morph
		neofsID    util.Uint160 // in morph

		alphabet alphabetContracts // in morph
	}

	chainParams struct {
		log  *zap.Logger
		cfg  *viper.Viper
		key  *keys.PrivateKey
		name string
		gas  util.Uint160
	}
)

const (
	morphPrefix   = "morph"
	mainnetPrefix = "mainnet"

	// extra blocks to overlap two deposits, we do that to make sure that
	// there won't be any blocks without deposited assets in notary contract;
	// make sure it is bigger than any extra rounding value in notary client.
	notaryExtraBlocks = 300
	// amount of tries before notary deposit timeout.
	notaryDepositTimeout = 100
)

var (
	errDepositTimeout = errors.New("notary deposit didn't appear in the network")
	errDepositFail    = errors.New("notary tx has faulted")
)

// Start runs all event providers.
func (s *Server) Start(ctx context.Context, intError chan<- error) (err error) {
	s.setHealthStatus(control.HealthStatus_STARTING)
	defer func() {
		if err == nil {
			s.setHealthStatus(control.HealthStatus_READY)
		}
	}()

	for _, starter := range s.starters {
		if err := starter(); err != nil {
			return err
		}
	}

	err = s.initConfigFromBlockchain()
	if err != nil {
		return err
	}

	if !s.mainNotaryConfig.disabled {
		err = s.initNotary(ctx,
			s.depositMainNotary,
			s.awaitMainNotaryDeposit,
			"waiting to accept main notary deposit",
		)
		if err != nil {
			return err
		}
	}

	if !s.sideNotaryConfig.disabled {
		err = s.initNotary(ctx,
			s.depositSideNotary,
			s.awaitSideNotaryDeposit,
			"waiting to accept side notary deposit",
		)
		if err != nil {
			return err
		}
	}

	// vote for sidechain validator if it is prepared in config
	err = s.voteForSidechainValidator(s.predefinedValidators)
	if err != nil {
		// we don't stop inner ring execution on this error
		s.log.Warn("can't vote for prepared validators",
			zap.String("error", err.Error()))
	}

	// tick initial epoch
	initialEpochTicker := timer.NewOneTickTimer(
		timer.StaticBlockMeter(s.initialEpochTickDelta),
		func() {
			s.netmapProcessor.HandleNewEpochTick(timerEvent.NewEpochTick{})
		})
	s.addBlockTimer(initialEpochTicker)

	morphErr := make(chan error)
	mainnnetErr := make(chan error)

	// anonymous function to multiplex error channels
	go func() {
		select {
		case <-ctx.Done():
			return
		case err := <-morphErr:
			intError <- fmt.Errorf("sidechain: %w", err)
		case err := <-mainnnetErr:
			intError <- fmt.Errorf("mainnet: %w", err)
		}
	}()

	s.morphListener.RegisterBlockHandler(func(b *block.Block) {
		s.log.Debug("new block",
			zap.Uint32("index", b.Index),
		)

		s.tickTimers()
	})

	for _, runner := range s.runners {
		runner(intError)
	}

	go s.morphListener.ListenWithError(ctx, morphErr)      // listen for neo:morph events
	go s.mainnetListener.ListenWithError(ctx, mainnnetErr) // listen for neo:mainnet events

	if err := s.startBlockTimers(); err != nil {
		return fmt.Errorf("could not start block timers: %w", err)
	}

	s.startWorkers(ctx)

	return nil
}

func (s *Server) startWorkers(ctx context.Context) {
	for _, w := range s.workers {
		go w(ctx)
	}
}

// Stop closes all subscription channels.
func (s *Server) Stop() {
	s.setHealthStatus(control.HealthStatus_SHUTTING_DOWN)

	go s.morphListener.Stop()
	go s.mainnetListener.Stop()

	for _, c := range s.closers {
		if err := c(); err != nil {
			s.log.Warn("closer error",
				zap.String("error", err.Error()),
			)
		}
	}
}

func (s *Server) registerNoErrCloser(c func()) {
	s.registerCloser(func() error {
		c()
		return nil
	})
}

func (s *Server) registerIOCloser(c io.Closer) {
	s.registerCloser(c.Close)
}

func (s *Server) registerCloser(f func() error) {
	s.closers = append(s.closers, f)
}

func (s *Server) registerStarter(f func() error) {
	s.starters = append(s.starters, f)
}

// New creates instance of inner ring sever structure.
func New(ctx context.Context, log *zap.Logger, cfg *viper.Viper) (*Server, error) {
	var err error
	server := &Server{log: log}

	server.setHealthStatus(control.HealthStatus_HEALTH_STATUS_UNDEFINED)

	// parse notary support
	server.feeConfig = config.NewFeeConfig(cfg)

	// prepare inner ring node private key
	acc, err := utilConfig.LoadAccount(
		cfg.GetString("wallet.path"),
		cfg.GetString("wallet.address"),
		cfg.GetString("wallet.password"))
	if err != nil {
		return nil, fmt.Errorf("ir: %w", err)
	}

	server.key = acc.PrivateKey()

	morphChain := &chainParams{
		log:  log,
		cfg:  cfg,
		key:  server.key,
		name: morphPrefix,
	}

	// create morph listener
	server.morphListener, err = createListener(ctx, morphChain)
	if err != nil {
		return nil, err
	}

	// create morph client
	server.morphClient, err = createClient(ctx, morphChain)
	if err != nil {
		return nil, err
	}

	withoutMainNet := cfg.GetBool("without_mainnet")

	if withoutMainNet {
		// This works as long as event Listener starts listening loop once,
		// otherwise Server.Start will run two similar routines.
		// This behavior most likely will not change.
		server.mainnetListener = server.morphListener
		server.mainnetClient = server.morphClient
	} else {
		mainnetChain := morphChain
		mainnetChain.name = mainnetPrefix

		// create mainnet listener
		server.mainnetListener, err = createListener(ctx, mainnetChain)
		if err != nil {
			return nil, err
		}

		// create mainnet client
		server.mainnetClient, err = createClient(ctx, mainnetChain)
		if err != nil {
			return nil, err
		}
	}

	server.mainNotaryConfig, server.sideNotaryConfig = parseNotaryConfigs(
		cfg,
		server.morphClient.ProbeNotary(),
		!withoutMainNet && server.mainnetClient.ProbeNotary(), // if mainnet disabled then notary flag must be disabled too
	)

	// get all script hashes of contracts
	server.contracts, err = parseContracts(
		cfg,
		withoutMainNet,
		server.mainNotaryConfig.disabled,
		server.sideNotaryConfig.disabled,
	)
	if err != nil {
		return nil, err
	}

	if !server.sideNotaryConfig.disabled {
		// enable notary support in the side client
		err = server.morphClient.EnableNotarySupport(
			client.WithProxyContract(server.contracts.proxy),
		)
		if err != nil {
			return nil, fmt.Errorf("could not enable side chain notary support: %w", err)
		}
	}

	if !server.mainNotaryConfig.disabled {
		// enable notary support in the main client
		err = server.mainnetClient.EnableNotarySupport(
			client.WithProxyContract(server.contracts.processing),
			client.WithAlphabetSource(server.morphClient.Committee),
		)
		if err != nil {
			return nil, fmt.Errorf("could not enable main chain notary support: %w", err)
		}
	}

	// parse default validators
	server.predefinedValidators, err = parsePredefinedValidators(cfg)
	if err != nil {
		return nil, fmt.Errorf("ir: can't parse predefined validators list: %w", err)
	}

	server.pubKey = server.key.PublicKey().Bytes()

	auditPool, err := ants.NewPool(cfg.GetInt("audit.task.exec_pool_size"))
	if err != nil {
		return nil, err
	}

	fee := server.feeConfig.SideChainFee()

	// do not use TryNotary() in audit wrapper
	// audit operations do not require multisignatures
	server.auditClient, err = auditWrapper.NewFromMorph(server.morphClient, server.contracts.audit, fee)
	if err != nil {
		return nil, err
	}

	cnrClient, err := cntWrapper.NewFromMorph(server.morphClient, server.contracts.container, fee, cntWrapper.TryNotary())
	if err != nil {
		return nil, err
	}

	server.netmapClient, err = nmWrapper.NewFromMorph(server.morphClient, server.contracts.netmap, fee, client.TryNotary())
	if err != nil {
		return nil, err
	}

	server.balanceClient, err = balanceWrapper.NewFromMorph(server.morphClient, server.contracts.balance, fee, balanceWrapper.TryNotary())
	if err != nil {
		return nil, err
	}

	repClient, err := repWrapper.NewFromMorph(server.morphClient, server.contracts.reputation, fee, repWrapper.TryNotary())
	if err != nil {
		return nil, err
	}

	neofsIDClient, err := neofsidWrapper.NewFromMorph(server.morphClient, server.contracts.neofsID, fee, neofsidWrapper.TryNotary())
	if err != nil {
		return nil, err
	}

	neofsClient, err := neofsWrapper.NewFromMorph(server.mainnetClient, server.contracts.neofs,
		server.feeConfig.MainChainFee(), neofsWrapper.TryNotary())
	if err != nil {
		return nil, err
	}

	var irf irFetcher

	if server.morphClient.ProbeNotary() {
		irf = NewIRFetcherWithNotary(server.morphClient)
	} else {
		irf = NewIRFetcherWithoutNotary(server.netmapClient)
	}

	server.statusIndex = newInnerRingIndexer(
		server.morphClient,
		irf,
		server.key.PublicKey(),
		cfg.GetDuration("indexer.cache_timeout"),
	)

	// create global runtime config reader
	globalConfig := config.NewGlobalConfigReader(cfg, server.netmapClient)

	clientCache := newClientCache(&clientCacheParams{
		Log:          log,
		Key:          &server.key.PrivateKey,
		SGTimeout:    cfg.GetDuration("audit.timeout.get"),
		HeadTimeout:  cfg.GetDuration("audit.timeout.head"),
		RangeTimeout: cfg.GetDuration("audit.timeout.rangehash"),
	})

	server.registerNoErrCloser(clientCache.cache.CloseAll)

	pdpPoolSize := cfg.GetInt("audit.pdp.pairs_pool_size")
	porPoolSize := cfg.GetInt("audit.por.pool_size")

	// create audit processor dependencies
	auditTaskManager := audittask.New(
		audittask.WithQueueCapacity(cfg.GetUint32("audit.task.queue_capacity")),
		audittask.WithWorkerPool(auditPool),
		audittask.WithLogger(log),
		audittask.WithContainerCommunicator(clientCache),
		audittask.WithMaxPDPSleepInterval(cfg.GetDuration("audit.pdp.max_sleep_interval")),
		audittask.WithPDPWorkerPoolGenerator(func() (util2.WorkerPool, error) {
			return ants.NewPool(pdpPoolSize)
		}),
		audittask.WithPoRWorkerPoolGenerator(func() (util2.WorkerPool, error) {
			return ants.NewPool(porPoolSize)
		}),
	)

	server.workers = append(server.workers, auditTaskManager.Listen)

	// create audit processor
	auditProcessor, err := audit.New(&audit.Params{
		Log:              log,
		NetmapClient:     server.netmapClient,
		ContainerClient:  cnrClient,
		IRList:           server,
		ClientCache:      clientCache,
		Key:              &server.key.PrivateKey,
		RPCSearchTimeout: cfg.GetDuration("audit.timeout.search"),
		TaskManager:      auditTaskManager,
		Reporter:         server,
	})
	if err != nil {
		return nil, err
	}

	// create settlement processor dependencies
	settlementDeps := &settlementDeps{
		globalConfig:  globalConfig,
		log:           server.log,
		cnrSrc:        cntWrapper.AsContainerSource(cnrClient),
		auditClient:   server.auditClient,
		nmSrc:         server.netmapClient,
		clientCache:   clientCache,
		balanceClient: server.balanceClient,
	}

	auditCalcDeps := &auditSettlementDeps{
		settlementDeps: settlementDeps,
	}

	basicSettlementDeps := &basicIncomeSettlementDeps{
		settlementDeps: settlementDeps,
		cnrClient:      cnrClient,
	}

	auditSettlementCalc := auditSettlement.NewCalculator(
		&auditSettlement.CalculatorPrm{
			ResultStorage:       auditCalcDeps,
			ContainerStorage:    auditCalcDeps,
			PlacementCalculator: auditCalcDeps,
			SGStorage:           auditCalcDeps,
			AccountStorage:      auditCalcDeps,
			Exchanger:           auditCalcDeps,
			AuditFeeFetcher:     auditCalcDeps,
		},
		auditSettlement.WithLogger(server.log),
	)

	// create settlement processor
	settlementProcessor := settlement.New(
		settlement.Prm{
			AuditProcessor: (*auditSettlementCalculator)(auditSettlementCalc),
			BasicIncome:    &basicSettlementConstructor{dep: basicSettlementDeps},
			State:          server,
		},
		settlement.WithLogger(server.log),
	)

	locodeValidator, err := server.newLocodeValidator(cfg)
	if err != nil {
		return nil, err
	}

	var alphaSync event.Handler

	if withoutMainNet {
		alphaSync = func(event.Event) {
			log.Debug("alphabet keys sync is disabled")
		}
	} else {
		// create governance processor
		governanceProcessor, err := governance.New(&governance.Params{
			Log:            log,
			NeoFSClient:    neofsClient,
			NetmapClient:   server.netmapClient,
			AlphabetState:  server,
			EpochState:     server,
			Voter:          server,
			IRFetcher:      irf,
			MorphClient:    server.morphClient,
			MainnetClient:  server.mainnetClient,
			NotaryDisabled: server.sideNotaryConfig.disabled,
		})
		if err != nil {
			return nil, err
		}

		alphaSync = governanceProcessor.HandleAlphabetSync
		err = bindMainnetProcessor(governanceProcessor, server)
		if err != nil {
			return nil, err
		}
	}

	// create netmap processor
	server.netmapProcessor, err = netmap.New(&netmap.Params{
		Log:              log,
		PoolSize:         cfg.GetInt("workers.netmap"),
		NetmapContract:   server.contracts.netmap,
		NetmapClient:     server.netmapClient,
		EpochTimer:       server,
		EpochState:       server,
		AlphabetState:    server,
		CleanupEnabled:   cfg.GetBool("netmap_cleaner.enabled"),
		CleanupThreshold: cfg.GetUint64("netmap_cleaner.threshold"),
		ContainerWrapper: cnrClient,
		HandleAudit: server.onlyActiveEventHandler(
			auditProcessor.StartAuditHandler(),
		),
		AuditSettlementsHandler: server.onlyAlphabetEventHandler(
			settlementProcessor.HandleAuditEvent,
		),
		AlphabetSyncHandler: alphaSync,
		NodeValidator: nodevalidator.New(
			addrvalidator.New(),
			locodeValidator,
		),
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(server.netmapProcessor, server)
	if err != nil {
		return nil, err
	}

	// container processor
	containerProcessor, err := container.New(&container.Params{
		Log:               log,
		PoolSize:          cfg.GetInt("workers.container"),
		ContainerContract: server.contracts.container,
		AlphabetState:     server,
		ContainerClient:   cnrClient,
		NeoFSIDClient:     neofsIDClient,
		NetworkState:      server.netmapClient,
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(containerProcessor, server)
	if err != nil {
		return nil, err
	}

	// create balance processor
	balanceProcessor, err := balance.New(&balance.Params{
		Log:             log,
		PoolSize:        cfg.GetInt("workers.balance"),
		NeoFSClient:     neofsClient,
		BalanceContract: server.contracts.balance,
		AlphabetState:   server,
		Converter:       &server.precision,
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(balanceProcessor, server)
	if err != nil {
		return nil, err
	}

	if !withoutMainNet {
		// create mainnnet neofs processor
		neofsProcessor, err := neofs.New(&neofs.Params{
			Log:                 log,
			PoolSize:            cfg.GetInt("workers.neofs"),
			NeoFSContract:       server.contracts.neofs,
			NeoFSIDClient:       neofsIDClient,
			BalanceClient:       server.balanceClient,
			NetmapClient:        server.netmapClient,
			MorphClient:         server.morphClient,
			EpochState:          server,
			AlphabetState:       server,
			Converter:           &server.precision,
			MintEmitCacheSize:   cfg.GetInt("emit.mint.cache_size"),
			MintEmitThreshold:   cfg.GetUint64("emit.mint.threshold"),
			MintEmitValue:       fixedn.Fixed8(cfg.GetInt64("emit.mint.value")),
			GasBalanceThreshold: cfg.GetInt64("emit.gas.balance_threshold"),
		})
		if err != nil {
			return nil, err
		}

		err = bindMainnetProcessor(neofsProcessor, server)
		if err != nil {
			return nil, err
		}
	}

	// create alphabet processor
	alphabetProcessor, err := alphabet.New(&alphabet.Params{
		Log:               log,
		PoolSize:          cfg.GetInt("workers.alphabet"),
		AlphabetContracts: server.contracts.alphabet,
		NetmapClient:      server.netmapClient,
		MorphClient:       server.morphClient,
		IRList:            server,
		StorageEmission:   cfg.GetUint64("emit.storage.amount"),
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(alphabetProcessor, server)
	if err != nil {
		return nil, err
	}

	// create reputation processor
	reputationProcessor, err := reputation.New(&reputation.Params{
		Log:                log,
		PoolSize:           cfg.GetInt("workers.reputation"),
		ReputationContract: server.contracts.reputation,
		EpochState:         server,
		AlphabetState:      server,
		ReputationWrapper:  repClient,
		ManagerBuilder: reputationcommon.NewManagerBuilder(
			reputationcommon.ManagersPrm{
				NetMapSource: server.netmapClient,
			},
		),
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(reputationProcessor, server)
	if err != nil {
		return nil, err
	}

	// todo: create vivid id component

	// initialize epoch timers
	server.epochTimer = newEpochTimer(&epochTimerArgs{
		l:                  server.log,
		nm:                 server.netmapProcessor,
		cnrWrapper:         cnrClient,
		epoch:              server,
		epochDuration:      globalConfig.EpochDuration,
		stopEstimationDMul: cfg.GetUint32("timers.stop_estimation.mul"),
		stopEstimationDDiv: cfg.GetUint32("timers.stop_estimation.div"),
		collectBasicIncome: subEpochEventHandler{
			handler:     settlementProcessor.HandleIncomeCollectionEvent,
			durationMul: cfg.GetUint32("timers.collect_basic_income.mul"),
			durationDiv: cfg.GetUint32("timers.collect_basic_income.div"),
		},
		distributeBasicIncome: subEpochEventHandler{
			handler:     settlementProcessor.HandleIncomeDistributionEvent,
			durationMul: cfg.GetUint32("timers.distribute_basic_income.mul"),
			durationDiv: cfg.GetUint32("timers.distribute_basic_income.div"),
		},
	})

	server.addBlockTimer(server.epochTimer)

	// initialize emission timer
	emissionTimer := newEmissionTimer(&emitTimerArgs{
		ap:           alphabetProcessor,
		emitDuration: cfg.GetUint32("timers.emit"),
	})

	server.addBlockTimer(emissionTimer)

	// initialize notary timers
	if !server.mainNotaryConfig.disabled {
		mainNotaryTimer := newNotaryDepositTimer(&notaryDepositArgs{
			l:              log,
			depositor:      server.depositMainNotary,
			notaryDuration: server.mainNotaryConfig.duration,
		})

		server.addBlockTimer(mainNotaryTimer)
	}

	if !server.sideNotaryConfig.disabled {
		sideNotaryTimer := newNotaryDepositTimer(&notaryDepositArgs{
			l:              log,
			depositor:      server.depositSideNotary,
			notaryDuration: server.sideNotaryConfig.duration,
		})

		server.addBlockTimer(sideNotaryTimer)
	}

	controlSvcEndpoint := cfg.GetString("control.grpc.endpoint")
	if controlSvcEndpoint != "" {
		authKeysStr := cfg.GetStringSlice("control.authorized_keys")
		authKeys := make([][]byte, 0, len(authKeysStr))

		for i := range authKeysStr {
			key, err := hex.DecodeString(authKeysStr[i])
			if err != nil {
				return nil, fmt.Errorf("could not parse Control authorized key %s: %w",
					authKeysStr[i],
					err,
				)
			}

			authKeys = append(authKeys, key)
		}

		var p controlsrv.Prm

		p.SetPrivateKey(*server.key)
		p.SetHealthChecker(server)

		controlSvc := controlsrv.New(p,
			controlsrv.WithAllowedKeys(authKeys),
		)

		grpcControlSrv := grpc.NewServer()
		control.RegisterControlServiceServer(grpcControlSrv, controlSvc)

		server.runners = append(server.runners, func(ch chan<- error) {
			lis, err := net.Listen("tcp", controlSvcEndpoint)
			if err != nil {
				ch <- err
				return
			}

			go func() {
				ch <- grpcControlSrv.Serve(lis)
			}()
		})

		server.registerNoErrCloser(grpcControlSrv.GracefulStop)
	} else {
		log.Info("no Control server endpoint specified, service is disabled")
	}

	return server, nil
}

func createListener(ctx context.Context, p *chainParams) (event.Listener, error) {
	sub, err := subscriber.New(ctx, &subscriber.Params{
		Log:         p.log,
		Endpoint:    p.cfg.GetString(p.name + ".endpoint.notification"),
		DialTimeout: p.cfg.GetDuration(p.name + ".dial_timeout"),
	})
	if err != nil {
		return nil, err
	}

	listener, err := event.NewListener(event.ListenerParams{
		Logger:     p.log,
		Subscriber: sub,
	})
	if err != nil {
		return nil, err
	}

	return listener, err
}

func createClient(ctx context.Context, p *chainParams) (*client.Client, error) {
	return client.New(
		p.key,
		p.cfg.GetString(p.name+".endpoint.client"),
		client.WithContext(ctx),
		client.WithLogger(p.log),
		client.WithDialTimeout(p.cfg.GetDuration(p.name+".dial_timeout")),
	)
}

func parseContracts(cfg *viper.Viper, withoutMainNet, withoutMainNotary, withoutSideNotary bool) (*contracts, error) {
	var (
		result = new(contracts)
		err    error
	)

	if !withoutMainNet {
		result.neofs, err = util.Uint160DecodeStringLE(cfg.GetString("contracts.neofs"))
		if err != nil {
			return nil, fmt.Errorf("ir: can't read neofs script-hash: %w", err)
		}

		if !withoutMainNotary {
			result.processing, err = util.Uint160DecodeStringLE(cfg.GetString("contracts.processing"))
			if err != nil {
				return nil, fmt.Errorf("ir: can't read processing script-hash: %w", err)
			}
		}
	}

	if !withoutSideNotary {
		result.proxy, err = util.Uint160DecodeStringLE(cfg.GetString("contracts.proxy"))
		if err != nil {
			return nil, fmt.Errorf("ir: can't read proxy script-hash: %w", err)
		}
	}

	netmapContractStr := cfg.GetString("contracts.netmap")
	balanceContractStr := cfg.GetString("contracts.balance")
	containerContractStr := cfg.GetString("contracts.container")
	auditContractStr := cfg.GetString("contracts.audit")
	reputationContractStr := cfg.GetString("contracts.reputation")
	neofsIDContractStr := cfg.GetString("contracts.neofsid")

	result.netmap, err = util.Uint160DecodeStringLE(netmapContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read netmap script-hash: %w", err)
	}

	result.balance, err = util.Uint160DecodeStringLE(balanceContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read balance script-hash: %w", err)
	}

	result.container, err = util.Uint160DecodeStringLE(containerContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read container script-hash: %w", err)
	}

	result.audit, err = util.Uint160DecodeStringLE(auditContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read audit script-hash: %w", err)
	}

	result.reputation, err = util.Uint160DecodeStringLE(reputationContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read reputation script-hash: %w", err)
	}

	result.neofsID, err = util.Uint160DecodeStringLE(neofsIDContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read NeoFS ID script-hash: %w", err)
	}

	result.alphabet, err = parseAlphabetContracts(cfg)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func parsePredefinedValidators(cfg *viper.Viper) (keys.PublicKeys, error) {
	publicKeyStrings := cfg.GetStringSlice("morph.validators")

	return ParsePublicKeysFromStrings(publicKeyStrings)
}

// ParsePublicKeysFromStrings returns slice of neo public keys from slice
// of hex encoded strings.
func ParsePublicKeysFromStrings(pubKeys []string) (keys.PublicKeys, error) {
	publicKeys := make(keys.PublicKeys, 0, len(pubKeys))

	for i := range pubKeys {
		key, err := keys.NewPublicKeyFromString(pubKeys[i])
		if err != nil {
			return nil, fmt.Errorf("can't decode public key: %w", err)
		}

		publicKeys = append(publicKeys, key)
	}

	return publicKeys, nil
}

func parseAlphabetContracts(cfg *viper.Viper) (alphabetContracts, error) {
	num := GlagoliticLetter(cfg.GetUint("contracts.alphabet.amount"))
	alpha := newAlphabetContracts()

	if num > lastLetterNum {
		return nil, fmt.Errorf("amount of alphabet contracts overflows glagolitsa %d > %d", num, lastLetterNum)
	}

	for letter := az; letter < num; letter++ {
		contractStr := cfg.GetString("contracts.alphabet." + letter.String())

		contractHash, err := util.Uint160DecodeStringLE(contractStr)
		if err != nil {
			return nil, fmt.Errorf("invalid alphabet %s contract: %s: %w", letter.String(), contractStr, err)
		}

		alpha.set(letter, contractHash)
	}

	return alpha, nil
}

func (s *Server) initConfigFromBlockchain() error {
	// get current epoch
	epoch, err := s.netmapClient.Epoch()
	if err != nil {
		return fmt.Errorf("can't parse epoch: %w", err)
	}

	// get balance precision
	balancePrecision, err := s.balanceClient.Decimals()
	if err != nil {
		return fmt.Errorf("can't read balance contract precision: %w", err)
	}

	// get next epoch delta tick
	s.initialEpochTickDelta, err = s.nextEpochBlockDelta()
	if err != nil {
		return err
	}

	s.epochCounter.Store(epoch)
	s.precision.SetBalancePrecision(balancePrecision)

	s.log.Debug("read config from blockchain",
		zap.Bool("active", s.IsActive()),
		zap.Bool("alphabet", s.IsAlphabet()),
		zap.Uint64("epoch", epoch),
		zap.Uint32("precision", balancePrecision),
		zap.Uint32("init_epoch_tick_delta", s.initialEpochTickDelta),
	)

	return nil
}

func (s *Server) nextEpochBlockDelta() (uint32, error) {
	epochBlock, err := s.netmapClient.LastEpochBlock()
	if err != nil {
		return 0, fmt.Errorf("can't read last epoch block: %w", err)
	}

	blockHeight, err := s.morphClient.BlockCount()
	if err != nil {
		return 0, fmt.Errorf("can't get side chain height: %w", err)
	}

	epochDuration, err := s.netmapClient.EpochDuration()
	if err != nil {
		return 0, fmt.Errorf("can't get epoch duration: %w", err)
	}

	delta := uint32(epochDuration) + epochBlock
	if delta < blockHeight {
		return 0, nil
	}

	return delta - blockHeight, nil
}

// onlyActiveHandler wrapper around event handler that executes it
// only if inner ring node state is active.
func (s *Server) onlyActiveEventHandler(f event.Handler) event.Handler {
	return func(ev event.Event) {
		if s.IsActive() {
			f(ev)
		}
	}
}

// onlyAlphabet wrapper around event handler that executes it
// only if inner ring node is alphabet node.
func (s *Server) onlyAlphabetEventHandler(f event.Handler) event.Handler {
	return func(ev event.Event) {
		if s.IsAlphabet() {
			f(ev)
		}
	}
}
