package innerring

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/balance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/container"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	auditSettlement "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/audit"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"github.com/nspcc-dev/neofs-node/pkg/morph/timer"
	audittask "github.com/nspcc-dev/neofs-node/pkg/services/audit/taskmanager"
	util2 "github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
	"go.uber.org/zap"
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

		// notary configuration
		feeConfig        *config.FeeConfig
		mainNotaryConfig *notaryConfig
		sideNotaryConfig *notaryConfig

		// internal variables
		key                  *ecdsa.PrivateKey
		pubKey               []byte
		contracts            *contracts
		predefinedValidators keys.PublicKeys

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

		alphabet alphabetContracts // in morph
	}

	chainParams struct {
		log  *zap.Logger
		cfg  *viper.Viper
		key  *ecdsa.PrivateKey
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
func (s *Server) Start(ctx context.Context, intError chan<- error) error {
	for _, starter := range s.starters {
		if err := starter(); err != nil {
			return err
		}
	}

	err := s.initConfigFromBlockchain()
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

	// parse notary support
	server.feeConfig = config.NewFeeConfig(cfg)
	server.mainNotaryConfig, server.sideNotaryConfig = parseNotaryConfigs(cfg)

	// prepare inner ring node private key
	server.key, err = crypto.LoadPrivateKey(cfg.GetString("key"))
	if err != nil {
		return nil, fmt.Errorf("ir: can't create private key: %w", err)
	}

	// get all script hashes of contracts
	server.contracts, err = parseContracts(cfg)
	if err != nil {
		return nil, err
	}

	// parse default validators
	server.predefinedValidators, err = parsePredefinedValidators(cfg)
	if err != nil {
		return nil, fmt.Errorf("ir: can't parse predefined validators list: %w", err)
	}

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

	// enable notary support in the client
	if !server.sideNotaryConfig.disabled {
		err = server.morphClient.EnableNotarySupport(server.contracts.proxy)
		if err != nil {
			return nil, err
		}
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

		// enable notary support in the client
		if !server.mainNotaryConfig.disabled {
			err = server.mainnetClient.EnableNotarySupport(
				server.contracts.processing,
				client.WithAlphabetSource(server.morphClient.Committee),
			)
			if err != nil {
				return nil, err
			}
		}
	}

	server.pubKey = crypto.MarshalPublicKey(&server.key.PublicKey)

	server.statusIndex = newInnerRingIndexer(
		server.morphClient,
		&server.key.PublicKey,
		cfg.GetDuration("indexer.cache_timeout"),
	)

	auditPool, err := ants.NewPool(cfg.GetInt("audit.task.exec_pool_size"))
	if err != nil {
		return nil, err
	}

	server.auditClient, err = invoke.NewAuditClient(server.morphClient, server.contracts.audit, server.feeConfig)
	if err != nil {
		return nil, err
	}

	cnrClient, err := invoke.NewContainerClient(server.morphClient, server.contracts.container, server.feeConfig)
	if err != nil {
		return nil, err
	}

	nmClient, err := invoke.NewNetmapClient(server.morphClient, server.contracts.netmap, server.feeConfig)
	if err != nil {
		return nil, err
	}

	balClient, err := invoke.NewBalanceClient(server.morphClient, server.contracts.balance, server.feeConfig)
	if err != nil {
		return nil, err
	}

	repClient, err := invoke.NewReputationClient(server.morphClient, server.contracts.reputation, server.feeConfig)
	if err != nil {
		return nil, err
	}

	// create global runtime config reader
	globalConfig := config.NewGlobalConfigReader(cfg, nmClient)

	clientCache := newClientCache(&clientCacheParams{
		Log:          log,
		Key:          server.key,
		SGTimeout:    cfg.GetDuration("audit.timeout.get"),
		HeadTimeout:  cfg.GetDuration("audit.timeout.head"),
		RangeTimeout: cfg.GetDuration("audit.timeout.rangehash"),
	})

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
		Log:               log,
		NetmapContract:    server.contracts.netmap,
		ContainerContract: server.contracts.container,
		AuditContract:     server.contracts.audit,
		MorphClient:       server.morphClient,
		IRList:            server,
		FeeProvider:       server.feeConfig,
		ClientCache:       clientCache,
		Key:               server.key,
		RPCSearchTimeout:  cfg.GetDuration("audit.timeout.search"),
		TaskManager:       auditTaskManager,
		Reporter:          server,
	})
	if err != nil {
		return nil, err
	}

	// create settlement processor dependencies
	settlementDeps := &settlementDeps{
		globalConfig:   globalConfig,
		log:            server.log,
		cnrSrc:         cnrClient,
		auditClient:    server.auditClient,
		nmSrc:          nmClient,
		clientCache:    clientCache,
		balanceClient:  balClient,
		notaryDisabled: server.sideNotaryConfig.disabled,
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

	// create governance processor
	governanceProcessor, err := governance.New(&governance.Params{
		Log:            log,
		NeoFSContract:  server.contracts.neofs,
		NetmapContract: server.contracts.netmap,
		AlphabetState:  server,
		EpochState:     server,
		Voter:          server,
		MorphClient:    server.morphClient,
		MainnetClient:  server.mainnetClient,
		NotaryDisabled: server.sideNotaryConfig.disabled,
		FeeProvider:    server.feeConfig,
	})
	if err != nil {
		return nil, err
	}

	var alphaSync event.Handler

	if withoutMainNet {
		alphaSync = func(event.Event) {
			log.Debug("alphabet keys sync is disabled")
		}
	} else {
		alphaSync = governanceProcessor.HandleAlphabetSync
		err = bindMainnetProcessor(governanceProcessor, server)
		if err != nil {
			return nil, err
		}
	}

	// create netmap processor
	netmapProcessor, err := netmap.New(&netmap.Params{
		Log:              log,
		PoolSize:         cfg.GetInt("workers.netmap"),
		NetmapContract:   server.contracts.netmap,
		EpochTimer:       server,
		MorphClient:      server.morphClient,
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
		NodeValidator:       locodeValidator,
		NotaryDisabled:      server.sideNotaryConfig.disabled,
		FeeProvider:         server.feeConfig,
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(netmapProcessor, server)
	if err != nil {
		return nil, err
	}

	// container processor
	containerProcessor, err := container.New(&container.Params{
		Log:               log,
		PoolSize:          cfg.GetInt("workers.container"),
		ContainerContract: server.contracts.container,
		MorphClient:       server.morphClient,
		AlphabetState:     server,
		FeeProvider:       server.feeConfig,
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
		NeoFSContract:   server.contracts.neofs,
		BalanceContract: server.contracts.balance,
		MainnetClient:   server.mainnetClient,
		AlphabetState:   server,
		Converter:       &server.precision,
		FeeProvider:     server.feeConfig,
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(balanceProcessor, server)
	if err != nil {
		return nil, err
	}

	// todo: create reputation processor

	// create mainnnet neofs processor
	neofsProcessor, err := neofs.New(&neofs.Params{
		Log:                 log,
		PoolSize:            cfg.GetInt("workers.neofs"),
		NeoFSContract:       server.contracts.neofs,
		BalanceContract:     server.contracts.balance,
		NetmapContract:      server.contracts.netmap,
		MorphClient:         server.morphClient,
		EpochState:          server,
		AlphabetState:       server,
		Converter:           &server.precision,
		FeeProvider:         server.feeConfig,
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

	// create alphabet processor
	alphabetProcessor, err := alphabet.New(&alphabet.Params{
		Log:               log,
		PoolSize:          cfg.GetInt("workers.alphabet"),
		AlphabetContracts: server.contracts.alphabet,
		NetmapContract:    server.contracts.netmap,
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
		NotaryDisabled:     server.sideNotaryConfig.disabled,
		ReputationContract: server.contracts.reputation,
		EpochState:         server,
		AlphabetState:      server,
		ReputationWrapper:  repClient,
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
		notaryDisabled:     server.sideNotaryConfig.disabled,
		nm:                 netmapProcessor,
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

func parseContracts(cfg *viper.Viper) (*contracts, error) {
	var (
		result = new(contracts)
		err    error
	)

	netmapContractStr := cfg.GetString("contracts.netmap")
	neofsContractStr := cfg.GetString("contracts.neofs")
	balanceContractStr := cfg.GetString("contracts.balance")
	containerContractStr := cfg.GetString("contracts.container")
	auditContractStr := cfg.GetString("contracts.audit")
	proxyContractStr := cfg.GetString("contracts.proxy")
	processingContractStr := cfg.GetString("contracts.processing")
	reputationContractStr := cfg.GetString("contracts.reputation")

	result.netmap, err = util.Uint160DecodeStringLE(netmapContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read netmap script-hash: %w", err)
	}

	result.neofs, err = util.Uint160DecodeStringLE(neofsContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read neofs script-hash: %w", err)
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

	result.proxy, err = util.Uint160DecodeStringLE(proxyContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read proxy script-hash: %w", err)
	}

	result.processing, err = util.Uint160DecodeStringLE(processingContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read processing script-hash: %w", err)
	}

	result.reputation, err = util.Uint160DecodeStringLE(reputationContractStr)
	if err != nil {
		return nil, fmt.Errorf("ir: can't read reputation script-hash: %w", err)
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
	num := glagoliticLetter(cfg.GetUint("contracts.alphabet.amount"))
	alpha := newAlphabetContracts()

	if num > lastLetterNum {
		return nil, fmt.Errorf("amount of alphabet contracts overflows glagolitsa %d > %d", num, lastLetterNum)
	}

	for letter := az; letter < num; letter++ {
		contractStr := cfg.GetString("contracts.alphabet." + letter.configString())

		contractHash, err := util.Uint160DecodeStringLE(contractStr)
		if err != nil {
			return nil, fmt.Errorf("invalid alphabet %s contract: %s: %w", letter.configString(), contractStr, err)
		}

		alpha.set(letter, contractHash)
	}

	return alpha, nil
}

func (s *Server) initConfigFromBlockchain() error {
	// get current epoch
	epoch, err := invoke.Epoch(s.morphClient, s.contracts.netmap)
	if err != nil {
		return fmt.Errorf("can't read epoch: %w", err)
	}

	// get balance precision
	balancePrecision, err := invoke.BalancePrecision(s.morphClient, s.contracts.balance)
	if err != nil {
		return fmt.Errorf("can't read balance contract precision: %w", err)
	}

	s.epochCounter.Store(uint64(epoch))
	s.precision.SetBalancePrecision(balancePrecision)

	s.log.Debug("read config from blockchain",
		zap.Bool("active", s.IsActive()),
		zap.Bool("alphabet", s.IsAlphabet()),
		zap.Int64("epoch", epoch),
		zap.Uint32("precision", balancePrecision),
	)

	return nil
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
