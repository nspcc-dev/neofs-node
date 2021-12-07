package innerring

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
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
	subnetvalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/subnet"
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
	morphsubnet "github.com/nspcc-dev/neofs-node/pkg/morph/client/subnet"
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
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
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
		epochDuration atomic.Uint64
		statusIndex   *innerRingIndexer
		precision     precision.Fixed8Converter
		auditClient   *auditWrapper.ClientWrapper
		healthStatus  atomic.Value
		balanceClient *balanceWrapper.Wrapper
		netmapClient  *nmWrapper.Wrapper
		persistate    *state.PersistentStorage

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
		withoutMainNet        bool

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

		subnetHandler
	}

	chainParams struct {
		log  *zap.Logger
		cfg  *viper.Viper
		key  *keys.PrivateKey
		name string
		gas  util.Uint160
		sgn  *transaction.Signer
		from uint32 // block height
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

	prm := governance.VoteValidatorPrm{}
	prm.Validators = s.predefinedValidators

	// vote for sidechain validator if it is prepared in config
	err = s.voteForSidechainValidator(prm)
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

		err = s.persistate.SetUInt32(persistateSideChainLastBlockKey, b.Index)
		if err != nil {
			s.log.Warn("can't update persistent state",
				zap.String("chain", "side"),
				zap.Uint32("block_index", b.Index))
		}

		s.tickTimers()
	})

	if !s.withoutMainNet {
		s.mainnetListener.RegisterBlockHandler(func(b *block.Block) {
			err = s.persistate.SetUInt32(persistateMainChainLastBlockKey, b.Index)
			if err != nil {
				s.log.Warn("can't update persistent state",
					zap.String("chain", "main"),
					zap.Uint32("block_index", b.Index))
			}
		})
	}

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

	server.persistate, err = initPersistentStateStorage(cfg)
	if err != nil {
		return nil, err
	}
	server.registerCloser(server.persistate.Close)

	fromSideChainBlock, err := server.persistate.UInt32(persistateSideChainLastBlockKey)
	if err != nil {
		fromSideChainBlock = 0
		log.Warn("can't get last processed side chain block number", zap.String("error", err.Error()))
	}

	morphChain := &chainParams{
		log:  log,
		cfg:  cfg,
		key:  server.key,
		name: morphPrefix,
		from: fromSideChainBlock,
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

	server.withoutMainNet = cfg.GetBool("without_mainnet")

	if server.withoutMainNet {
		// This works as long as event Listener starts listening loop once,
		// otherwise Server.Start will run two similar routines.
		// This behavior most likely will not change.
		server.mainnetListener = server.morphListener
		server.mainnetClient = server.morphClient
	} else {
		mainnetChain := morphChain
		mainnetChain.name = mainnetPrefix
		mainnetChain.sgn = &transaction.Signer{Scopes: transaction.CalledByEntry}

		fromMainChainBlock, err := server.persistate.UInt32(persistateMainChainLastBlockKey)
		if err != nil {
			fromMainChainBlock = 0
			log.Warn("can't get last processed main chain block number", zap.String("error", err.Error()))
		}
		mainnetChain.from = fromMainChainBlock

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
		!server.withoutMainNet && server.mainnetClient.ProbeNotary(), // if mainnet disabled then notary flag must be disabled too
	)

	log.Debug("notary support",
		zap.Bool("sidechain_enabled", !server.sideNotaryConfig.disabled),
		zap.Bool("mainchain_enabled", !server.mainNotaryConfig.disabled),
	)

	// get all script hashes of contracts
	server.contracts, err = parseContracts(
		cfg,
		server.morphClient,
		server.withoutMainNet,
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

		server.morphListener.EnableNotarySupport(server.contracts.proxy, server.morphClient.Committee, server.morphClient)
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

	// form morph container client's options
	morphCnrOpts := make([]cntWrapper.Option, 0, 3)
	morphCnrOpts = append(morphCnrOpts,
		cntWrapper.TryNotary(),
		cntWrapper.AsAlphabet(),
	)

	if server.sideNotaryConfig.disabled {
		// in non-notary environments we customize fee for named container registration
		// because it takes much more additional GAS than other operations.
		morphCnrOpts = append(morphCnrOpts,
			cntWrapper.WithCustomFeeForNamedPut(server.feeConfig.NamedContainerRegistrationFee()),
		)
	}

	cnrClient, err := cntWrapper.NewFromMorph(server.morphClient, server.contracts.container, fee, morphCnrOpts...)
	if err != nil {
		return nil, err
	}

	server.netmapClient, err = nmWrapper.NewFromMorph(server.morphClient, server.contracts.netmap, fee, nmWrapper.TryNotary(), nmWrapper.AsAlphabet())
	if err != nil {
		return nil, err
	}

	server.balanceClient, err = balanceWrapper.NewFromMorph(server.morphClient, server.contracts.balance, fee, balanceWrapper.TryNotary(), balanceWrapper.AsAlphabet())
	if err != nil {
		return nil, err
	}

	repClient, err := repWrapper.NewFromMorph(server.morphClient, server.contracts.reputation, fee, repWrapper.TryNotary(), repWrapper.AsAlphabet())
	if err != nil {
		return nil, err
	}

	neofsIDClient, err := neofsidWrapper.NewFromMorph(server.morphClient, server.contracts.neofsID, fee, neofsidWrapper.TryNotary(), neofsidWrapper.AsAlphabet())
	if err != nil {
		return nil, err
	}

	neofsClient, err := neofsWrapper.NewFromMorph(server.mainnetClient, server.contracts.neofs,
		server.feeConfig.MainChainFee(), neofsWrapper.TryNotary(), neofsWrapper.AsAlphabet())
	if err != nil {
		return nil, err
	}

	// initialize morph client of Subnet contract
	clientMode := morphsubnet.NotaryAlphabet

	if server.sideNotaryConfig.disabled {
		clientMode = morphsubnet.NonNotary
	}

	subnetInitPrm := morphsubnet.InitPrm{}
	subnetInitPrm.SetBaseClient(server.morphClient)
	subnetInitPrm.SetContractAddress(server.contracts.subnet)
	subnetInitPrm.SetMode(clientMode)

	subnetClient := &morphsubnet.Client{}
	err = subnetClient.Init(subnetInitPrm)
	if err != nil {
		return nil, fmt.Errorf("could not initialize subnet client: %w", err)
	}

	var irf irFetcher

	if !server.mainNotaryConfig.disabled {
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
		SGSource:         clientCache,
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

	subnetValidator, err := subnetvalidator.New(
		subnetvalidator.Prm{
			SubnetClient: subnetClient,
		},
	)
	if err != nil {
		return nil, err
	}

	var alphaSync event.Handler

	if server.withoutMainNet || cfg.GetBool("governance.disable") {
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
		NotaryDepositHandler: server.onlyAlphabetEventHandler(
			server.notaryHandler,
		),
		AuditSettlementsHandler: server.onlyAlphabetEventHandler(
			settlementProcessor.HandleAuditEvent,
		),
		AlphabetSyncHandler: alphaSync,
		NodeValidator: nodevalidator.New(
			addrvalidator.New(),
			locodeValidator,
			subnetValidator,
		),
		NotaryDisabled: server.sideNotaryConfig.disabled,
		SubnetContract: &server.contracts.subnet,
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
		Log:             log,
		PoolSize:        cfg.GetInt("workers.container"),
		AlphabetState:   server,
		ContainerClient: cnrClient,
		NeoFSIDClient:   neofsIDClient,
		NetworkState:    server.netmapClient,
		NotaryDisabled:  server.sideNotaryConfig.disabled,
		SubnetClient:    subnetClient,
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
		Log:           log,
		PoolSize:      cfg.GetInt("workers.balance"),
		NeoFSClient:   neofsClient,
		AlphabetState: server,
		Converter:     &server.precision,
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(balanceProcessor, server)
	if err != nil {
		return nil, err
	}

	if !server.withoutMainNet {
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
		Log:               log,
		PoolSize:          cfg.GetInt("workers.reputation"),
		EpochState:        server,
		AlphabetState:     server,
		ReputationWrapper: repClient,
		ManagerBuilder: reputationcommon.NewManagerBuilder(
			reputationcommon.ManagersPrm{
				NetMapSource: server.netmapClient,
			},
		),
		NotaryDisabled: server.sideNotaryConfig.disabled,
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
		newEpochHandlers:   server.newEpochTickHandlers(),
		cnrWrapper:         cnrClient,
		epoch:              server,
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

	server.initSubnet(subnetConfig{
		queueSize: cfg.GetUint32("workers.subnet"),
	})

	return server, nil
}

func createListener(ctx context.Context, p *chainParams) (event.Listener, error) {
	// config name left unchanged for compatibility, may be its better to rename it to "endpoints"
	endpoints := p.cfg.GetStringSlice(p.name + ".endpoint.notification")
	if len(endpoints) == 0 {
		return nil, errors.New("missing morph notification endpoints")
	}

	var (
		sub subscriber.Subscriber
		err error
	)

	dialTimeout := p.cfg.GetDuration(p.name + ".dial_timeout")

	for i := range endpoints {
		sub, err = subscriber.New(ctx, &subscriber.Params{
			Log:            p.log,
			Endpoint:       endpoints[i],
			DialTimeout:    dialTimeout,
			StartFromBlock: p.from,
		})
		if err == nil {
			break
		}

		p.log.Info("failed to establish websocket neo event listener, trying another",
			zap.String("endpoint", endpoints[i]),
			zap.String("error", err.Error()),
		)
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
	// config name left unchanged for compatibility, may be its better to rename it to "endpoints" or "clients"
	endpoints := p.cfg.GetStringSlice(p.name + ".endpoint.client")
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%s chain client endpoints not provided", p.name)
	}

	return client.New(
		p.key,
		endpoints[0],
		client.WithContext(ctx),
		client.WithLogger(p.log),
		client.WithDialTimeout(p.cfg.GetDuration(p.name+".dial_timeout")),
		client.WithSigner(p.sgn),
		client.WithExtraEndpoints(endpoints[1:]),
	)
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

func (s *Server) initConfigFromBlockchain() error {
	// get current epoch
	epoch, err := s.netmapClient.Epoch()
	if err != nil {
		return fmt.Errorf("can't read epoch number: %w", err)
	}

	// get current epoch duration
	epochDuration, err := s.netmapClient.EpochDuration()
	if err != nil {
		return fmt.Errorf("can't read epoch duration: %w", err)
	}

	// get balance precision
	balancePrecision, err := s.balanceClient.Decimals()
	if err != nil {
		return fmt.Errorf("can't read balance contract precision: %w", err)
	}

	s.epochCounter.Store(epoch)
	s.epochDuration.Store(epochDuration)
	s.precision.SetBalancePrecision(balancePrecision)

	// get next epoch delta tick
	s.initialEpochTickDelta, err = s.nextEpochBlockDelta()
	if err != nil {
		return err
	}

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

	delta := uint32(s.epochDuration.Load()) + epochBlock
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

func (s *Server) newEpochTickHandlers() []newEpochHandler {
	newEpochHandlers := []newEpochHandler{
		func() {
			s.netmapProcessor.HandleNewEpochTick(timerEvent.NewEpochTick{})
		},
	}

	return newEpochHandlers
}
