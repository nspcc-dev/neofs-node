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
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/balance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/container"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	nodevalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation"
	addrvalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/maddress"
	statevalidation "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/state"
	subnetvalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/subnet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	auditSettlement "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/audit"
	timerEvent "github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	balanceClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	neofsClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	repClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
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
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	// Server is the inner ring application structure that contains all event
	// processors, shared variables and event handlers.
	Server struct {
		log *logger.Logger

		bc *blockchain.Blockchain

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
		auditClient   *auditClient.Client
		healthStatus  atomic.Value
		balanceClient *balanceClient.Client
		netmapClient  *nmClient.Client
		persistate    *state.PersistentStorage

		// metrics
		metrics *metrics.InnerRingServiceMetrics

		// notary configuration
		feeConfig        *config.FeeConfig
		mainNotaryConfig *notaryConfig

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
		runners []func(chan<- error) error

		subnetHandler
	}

	chainParams struct {
		log  *logger.Logger
		cfg  *viper.Viper
		key  *keys.PrivateKey
		name string
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

	err = s.initNotary(ctx,
		s.depositSideNotary,
		s.awaitSideNotaryDeposit,
		"waiting to accept side notary deposit",
	)
	if err != nil {
		return err
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

		s.tickTimers(b.Index)
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
		if err := runner(intError); err != nil {
			return err
		}
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

	if s.bc != nil {
		s.bc.Stop()
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
func New(ctx context.Context, log *logger.Logger, cfg *viper.Viper, errChan chan<- error) (*Server, error) {
	var err error
	server := &Server{log: log}

	server.setHealthStatus(control.HealthStatus_HEALTH_STATUS_UNDEFINED)

	// parse notary support
	server.feeConfig = config.NewFeeConfig(cfg)

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
		name: morphPrefix,
		from: fromSideChainBlock,
	}

	const walletPathKey = "wallet.path"
	if !cfg.IsSet(walletPathKey) {
		return nil, fmt.Errorf("file path to the node Neo wallet is not configured '%s'", walletPathKey)
	}

	walletPath := cfg.GetString(walletPathKey)
	walletPass := cfg.GetString("wallet.password")

	// parse default validators
	server.predefinedValidators, err = parsePredefinedValidators(cfg)
	if err != nil {
		return nil, fmt.Errorf("can't parse predefined validators list: %w", err)
	}

	// create morph client
	if isLocalConsensusMode(cfg) {
		// go on a local blockchain
		cfgBlockchain, err := parseBlockchainConfig(cfg, log)
		if err != nil {
			return nil, fmt.Errorf("invalid blockchain configuration: %w", err)
		}

		if len(server.predefinedValidators) == 0 {
			server.predefinedValidators = cfgBlockchain.Committee
		}

		cfgBlockchain.Wallet.Path = walletPath
		cfgBlockchain.Wallet.Password = walletPass
		cfgBlockchain.ErrorListener = errChan

		server.bc, err = blockchain.New(cfgBlockchain)
		if err != nil {
			return nil, fmt.Errorf("init internal blockchain: %w", err)
		}

		// don't move this code area within current function
		{
			log.Info("running blockchain...")
			// we run node here instead of Server.Start because some Server components
			// require blockchain to be run in the current function. Later it would be nice
			// to separate the stages of construction and launch. Track neofs-node#2294
			err = server.bc.Run(ctx)
			if err != nil {
				return nil, fmt.Errorf("run internal blockchain: %w", err)
			}

			defer func() {
				if err != nil {
					server.bc.Stop()
				}
			}()
		}

		wsClient, err := server.bc.BuildWSClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("build WS client on internal blockchain: %w", err)
		}

		server.key = server.bc.LocalKey()
		morphChain.key = server.key

		server.morphClient, err = client.New(
			server.key,
			client.WithContext(ctx),
			client.WithLogger(log),
			client.WithSingleClient(wsClient),
		)
		if err != nil {
			return nil, fmt.Errorf("init internal morph client: %w", err)
		}
	} else {
		if len(server.predefinedValidators) == 0 {
			return nil, fmt.Errorf("empty '%s' list in config", validatorsConfigKey)
		}

		// fallback to the pure RPC architecture
		acc, err := utilConfig.LoadAccount(
			walletPath,
			cfg.GetString("wallet.address"),
			walletPass,
		)
		if err != nil {
			return nil, fmt.Errorf("ir: %w", err)
		}

		server.key = acc.PrivateKey()
		morphChain.key = server.key

		server.morphClient, err = createClient(ctx, morphChain, errChan)
		if err != nil {
			return nil, err
		}
	}

	// create morph listener
	server.morphListener, err = createListener(ctx, server.morphClient, morphChain)
	if err != nil {
		return nil, err
	}
	if err := server.morphClient.SetGroupSignerScope(); err != nil {
		morphChain.log.Info("failed to set group signer scope, continue with Global", zap.Error(err))
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

		// create mainnet client
		server.mainnetClient, err = createClient(ctx, mainnetChain, errChan)
		if err != nil {
			return nil, err
		}

		// create mainnet listener
		server.mainnetListener, err = createListener(ctx, server.mainnetClient, mainnetChain)
		if err != nil {
			return nil, err
		}
	}

	server.mainNotaryConfig = new(notaryConfig)
	server.mainNotaryConfig.disabled = server.withoutMainNet || !server.mainnetClient.ProbeNotary() // if mainnet disabled then notary flag must be disabled too

	log.Info("notary support",
		zap.Bool("mainchain_enabled", !server.mainNotaryConfig.disabled),
	)

	// get all script hashes of contracts
	server.contracts, err = initContracts(ctx, log,
		cfg,
		server.morphClient,
		server.withoutMainNet,
		server.mainNotaryConfig.disabled,
	)
	if err != nil {
		return nil, err
	}

	// enable notary support in the side client
	err = server.morphClient.EnableNotarySupport(
		client.WithProxyContract(server.contracts.proxy),
	)
	if err != nil {
		return nil, fmt.Errorf("could not enable side chain notary support: %w", err)
	}

	server.morphListener.EnableNotarySupport(server.contracts.proxy, server.morphClient.Committee, server.morphClient)

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

	server.pubKey = server.key.PublicKey().Bytes()

	auditPool, err := ants.NewPool(cfg.GetInt("audit.task.exec_pool_size"))
	if err != nil {
		return nil, err
	}

	// do not use TryNotary() in audit wrapper
	// audit operations do not require multisignatures
	server.auditClient, err = auditClient.NewFromMorph(server.morphClient, server.contracts.audit, 0)
	if err != nil {
		return nil, err
	}

	// form morph container client's options
	morphCnrOpts := make([]cntClient.Option, 0, 3)
	morphCnrOpts = append(morphCnrOpts,
		cntClient.AsAlphabet(),
	)

	cnrClient, err := cntClient.NewFromMorph(server.morphClient, server.contracts.container, 0, morphCnrOpts...)
	if err != nil {
		return nil, err
	}

	server.netmapClient, err = nmClient.NewFromMorph(server.morphClient, server.contracts.netmap, 0, nmClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	server.balanceClient, err = balanceClient.NewFromMorph(server.morphClient, server.contracts.balance, 0, balanceClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	repClient, err := repClient.NewFromMorph(server.morphClient, server.contracts.reputation, 0, repClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	neofsIDClient, err := neofsid.NewFromMorph(server.morphClient, server.contracts.neofsID, 0, neofsid.AsAlphabet())
	if err != nil {
		return nil, err
	}

	neofsCli, err := neofsClient.NewFromMorph(server.mainnetClient, server.contracts.neofs,
		server.feeConfig.MainChainFee(), neofsClient.TryNotary(), neofsClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	// initialize morph client of Subnet contract
	clientMode := morphsubnet.NotaryAlphabet

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

	if server.withoutMainNet {
		// if mainchain is disabled we should use NeoFSAlphabetList client method according to its docs
		// (naming `...WithNotary` will not always be correct)
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

	clientCache := newClientCache(&clientCacheParams{
		Log:           log,
		Key:           &server.key.PrivateKey,
		SGTimeout:     cfg.GetDuration("audit.timeout.get"),
		HeadTimeout:   cfg.GetDuration("audit.timeout.head"),
		RangeTimeout:  cfg.GetDuration("audit.timeout.rangehash"),
		AllowExternal: cfg.GetBool("audit.allow_external"),
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
		EpochSource:      server,
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
	settlementDeps := settlementDeps{
		log:           server.log,
		cnrSrc:        cntClient.AsContainerSource(cnrClient),
		auditClient:   server.auditClient,
		nmClient:      server.netmapClient,
		clientCache:   clientCache,
		balanceClient: server.balanceClient,
	}

	settlementDeps.settlementCtx = auditSettlementContext
	auditCalcDeps := &auditSettlementDeps{
		settlementDeps: settlementDeps,
	}

	settlementDeps.settlementCtx = basicIncomeSettlementContext
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
			AuditFeeFetcher:     server.netmapClient,
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
			Log:           log,
			NeoFSClient:   neofsCli,
			NetmapClient:  server.netmapClient,
			AlphabetState: server,
			EpochState:    server,
			Voter:         server,
			IRFetcher:     irf,
			MorphClient:   server.morphClient,
			MainnetClient: server.mainnetClient,
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

	netSettings := (*networkSettings)(server.netmapClient)

	var netMapCandidateStateValidator statevalidation.NetMapCandidateValidator
	netMapCandidateStateValidator.SetNetworkSettings(netSettings)

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
			&netMapCandidateStateValidator,
			addrvalidator.New(),
			locodeValidator,
			subnetValidator,
		),
		SubnetContract: &server.contracts.subnet,

		NodeStateSettings: netSettings,
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
		NeoFSClient:   neofsCli,
		BalanceSC:     server.contracts.balance,
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
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(reputationProcessor, server)
	if err != nil {
		return nil, err
	}

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

		server.runners = append(server.runners, func(ch chan<- error) error {
			lis, err := net.Listen("tcp", controlSvcEndpoint)
			if err != nil {
				return err
			}

			go func() {
				ch <- grpcControlSrv.Serve(lis)
			}()
			return nil
		})

		server.registerNoErrCloser(grpcControlSrv.GracefulStop)
	} else {
		log.Info("no Control server endpoint specified, service is disabled")
	}

	server.initSubnet(subnetConfig{
		queueSize: cfg.GetUint32("workers.subnet"),
	})

	if cfg.GetString("prometheus.address") != "" {
		m := metrics.NewInnerRingMetrics(misc.Version)
		server.metrics = &m
	}

	return server, nil
}

func createListener(ctx context.Context, cli *client.Client, p *chainParams) (event.Listener, error) {
	// listenerPoolCap is a capacity of a
	// worker pool inside the listener. It
	// is used to prevent blocking in neo-go:
	// the client cannot make RPC requests if
	// the notification channel is not being
	// read by another goroutine.
	const listenerPoolCap = 10

	var (
		sub subscriber.Subscriber
		err error
	)

	sub, err = subscriber.New(ctx, &subscriber.Params{
		Log:            p.log,
		StartFromBlock: p.from,
		Client:         cli,
	})
	if err != nil {
		return nil, err
	}

	listener, err := event.NewListener(event.ListenerParams{
		Logger:             &logger.Logger{Logger: p.log.With(zap.String("chain", p.name))},
		Subscriber:         sub,
		WorkerPoolCapacity: listenerPoolCap,
	})
	if err != nil {
		return nil, err
	}

	return listener, err
}

func createClient(ctx context.Context, p *chainParams, errChan chan<- error) (*client.Client, error) {
	endpoints := p.cfg.GetStringSlice(p.name + ".endpoints")

	// deprecated endpoints with priorities
	section := p.name + ".endpoint.client"
	for i := 0; ; i++ {
		addr := p.cfg.GetString(fmt.Sprintf("%s.%d.%s", section, i, "address"))
		if addr == "" {
			break
		}

		endpoints = append(endpoints, addr)
	}

	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%s chain client endpoints not provided", p.name)
	}

	return client.New(
		p.key,
		client.WithContext(ctx),
		client.WithLogger(p.log),
		client.WithDialTimeout(p.cfg.GetDuration(p.name+".dial_timeout")),
		client.WithSigner(p.sgn),
		client.WithEndpoints(endpoints),
		client.WithReconnectionRetries(p.cfg.GetInt(p.name+".reconnections_number")),
		client.WithReconnectionsDelay(p.cfg.GetDuration(p.name+".reconnections_delay")),
		client.WithConnLostCallback(func() {
			errChan <- fmt.Errorf("%s chain connection has been lost", p.name)
		}),
	)
}

const validatorsConfigKey = "morph.validators"

func parsePredefinedValidators(cfg *viper.Viper) (keys.PublicKeys, error) {
	publicKeyStrings := cfg.GetStringSlice(validatorsConfigKey)

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
