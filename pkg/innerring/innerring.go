package innerring

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-contract/deploy"
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
	availabilityvalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/availability"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/privatedomains"
	statevalidation "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/state"
	addrvalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/structure"
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
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	repClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/timer"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	audittask "github.com/nspcc-dev/neofs-node/pkg/services/audit/taskmanager"
	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	controlsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	util2 "github.com/nspcc-dev/neofs-node/pkg/util"
	utilConfig "github.com/nspcc-dev/neofs-node/pkg/util/config"
	"github.com/nspcc-dev/neofs-node/pkg/util/glagolitsa"
	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/panjf2000/ants/v2"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	// Server is the inner ring application structure that contains all event
	// processors, shared variables and event handlers.
	Server struct {
		log *zap.Logger

		bc *blockchain.Blockchain

		// event producers
		morphListener   event.Listener
		mainnetListener event.Listener
		epochTimer      *timer.BlockTimer
		initEpochTimer  atomic.Pointer[timer.BlockTimer]

		morphClient   *client.Client
		mainnetClient *client.Client
		auditClient   *auditClient.Client
		balanceClient *balanceClient.Client
		netmapClient  *nmClient.Client

		auditTaskManager *audittask.Manager

		// global state
		epochCounter  atomic.Uint64
		epochDuration atomic.Uint64
		statusIndex   *innerRingIndexer
		precision     uint32 // not changeable
		healthStatus  atomic.Value
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
		initialEpochTickDelta atomic.Uint32
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
	}

	chainParams struct {
		log  *zap.Logger
		cfg  *viper.Viper
		key  *keys.PrivateKey
		name string
		from uint32 // block height

		withAutoFSChainScope bool
	}
)

const (
	deprecatedMorphPrefix = "morph"
	fsChainPrefix         = "fschain"
	mainnetPrefix         = "mainnet"

	// extra blocks to overlap two deposits, we do that to make sure that
	// there won't be any blocks without deposited assets in notary contract;
	// make sure it is bigger than any extra rounding value in notary client.
	notaryExtraBlocks = 300
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
		err = s.depositMainNotary()
		if err != nil {
			return fmt.Errorf("main notary deposit: %w", err)
		}

		s.log.Info("made main chain notary deposit successfully")
	}

	err = s.depositFSNotary()
	if err != nil {
		return fmt.Errorf("fs chain notary deposit: %w", err)
	}

	s.log.Info("made fs chain notary deposit successfully")

	// vote for FS chain validator if it is prepared in config
	err = s.voteForFSChainValidator(s.predefinedValidators, nil)
	if err != nil {
		// we don't stop inner ring execution on this error
		s.log.Warn("can't vote for prepared validators",
			zap.Error(err))
	}

	// tick initial epoch
	initialEpochTicker := timer.NewOneTickTimer(
		func() (uint32, error) {
			return s.initialEpochTickDelta.Load(), nil
		},
		func() {
			s.netmapProcessor.HandleNewEpochTick(timerEvent.NewEpochTick{})
		})
	s.initEpochTimer.Store(initialEpochTicker)

	morphErr := make(chan error)
	mainnnetErr := make(chan error)

	// anonymous function to multiplex error channels
	go func() {
		select {
		case <-ctx.Done():
			return
		case err := <-morphErr:
			intError <- fmt.Errorf("FS chain: %w", err)
		case err := <-mainnnetErr:
			intError <- fmt.Errorf("mainnet: %w", err)
		}
	}()

	s.morphListener.RegisterHeaderHandler(func(b *block.Header) {
		s.log.Debug("new block",
			zap.Uint32("index", b.Index),
		)

		err = s.persistate.SetUInt32(persistateFSChainLastBlockKey, b.Index)
		if err != nil {
			s.log.Warn("can't update persistent state",
				zap.String("chain", "FS"),
				zap.Uint32("block_index", b.Index))
		}

		s.epochTimer.Tick(b.Index)
		if initEpochTimer := s.initEpochTimer.Load(); initEpochTimer != nil {
			initEpochTimer.Tick(b.Index)
		}
	})

	if !s.withoutMainNet {
		s.mainnetListener.RegisterHeaderHandler(func(b *block.Header) {
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

	if err = s.epochTimer.Reset(); err != nil {
		return fmt.Errorf("could not start new epoch block timer: %w", err)
	}
	if err = initialEpochTicker.Reset(); err != nil {
		return fmt.Errorf("could not start initial new epoch block timer: %w", err)
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
				zap.Error(err),
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

func (s *Server) registerCloser(f func() error) {
	s.closers = append(s.closers, f)
}

// New creates instance of inner ring server structure.
func New(ctx context.Context, log *zap.Logger, cfg *viper.Viper, errChan chan<- error) (*Server, error) {
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

	fromDeprectedSidechanBlock, err := server.persistate.UInt32(persistateDeprecatedSidechainLastBlockKey)
	if err != nil {
		fromDeprectedSidechanBlock = 0
	}
	fromFSChainBlock, err := server.persistate.UInt32(persistateFSChainLastBlockKey)
	if err != nil {
		fromFSChainBlock = 0
		log.Warn("can't get last processed FS chain block number", zap.Error(err))
	}

	// migration for deprecated DB key
	if fromFSChainBlock == 0 && fromDeprectedSidechanBlock != fromFSChainBlock {
		fromFSChainBlock = fromDeprectedSidechanBlock
		err = server.persistate.SetUInt32(persistateFSChainLastBlockKey, fromFSChainBlock)
		if err != nil {
			log.Warn("can't update persistent state",
				zap.String("chain", "FS"),
				zap.Uint32("block_index", fromFSChainBlock))
		}

		err = server.persistate.Delete(persistateDeprecatedSidechainLastBlockKey)
		if err != nil {
			log.Warn("can't delete deprecated persistent state", zap.Error(err))
		}
	}

	if cfg.IsSet(deprecatedMorphPrefix+".endpoints") || cfg.IsSet(deprecatedMorphPrefix+".consensus") {
		log.Warn("config section 'morph' is deprecated, use 'fschain'")
		cfgPathFSChain = deprecatedMorphPrefix
	}
	if cfg.IsSet(fsChainPrefix+".endpoints") || cfg.IsSet(fsChainPrefix+".consensus") {
		cfgPathFSChain = fsChainPrefix
	}
	cfgPathFSChainRPCEndpoints = cfgPathFSChain + ".endpoints"
	cfgPathFSChainLocalConsensus = cfgPathFSChain + ".consensus"
	cfgPathFSChainValidators = cfgPathFSChain + ".validators"
	fsChainParams := chainParams{
		log:  log,
		cfg:  cfg,
		name: cfgPathFSChain,
		from: fromFSChainBlock,
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

	wlt, err := wallet.NewWalletFromFile(walletPath)
	if err != nil {
		return nil, fmt.Errorf("read wallet from file '%s': %w", walletPath, err)
	}

	const singleAccLabel = "single"
	const consensusAccLabel = "consensus"
	var singleAcc *wallet.Account
	var consensusAcc *wallet.Account

	for i := range wlt.Accounts {
		err = wlt.Accounts[i].Decrypt(walletPass, keys.NEP2ScryptParams())
		switch wlt.Accounts[i].Label {
		case singleAccLabel:
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt account with label '%s' in wallet '%s': %w", singleAccLabel, walletPass, err)
			}

			singleAcc = wlt.Accounts[i]
		case consensusAccLabel:
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt account with label '%s' in wallet '%s': %w", consensusAccLabel, walletPass, err)
			}

			consensusAcc = wlt.Accounts[i]
		}
	}

	isAutoDeploy, err := isAutoDeploymentMode(cfg)
	if err != nil {
		return nil, err
	}

	isLocalConsensus, err := isLocalConsensusMode(cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid consensus configuration: %w", err)
	}

	if isLocalConsensus {
		if singleAcc == nil {
			return nil, fmt.Errorf("missing account with label '%s' in wallet '%s'", singleAccLabel, walletPass)
		}

		server.key = singleAcc.PrivateKey()
	} else {
		acc, err := utilConfig.LoadAccount(walletPath, cfg.GetString("wallet.address"), walletPass)
		if err != nil {
			return nil, fmt.Errorf("ir: %w", err)
		}

		server.key = acc.PrivateKey()
	}

	err = serveControl(server, log, cfg, errChan)
	if err != nil {
		return nil, err
	}

	serveMetrics(server, cfg)

	var localWSClient *rpcclient.WSClient // set if isLocalConsensus only

	// create morph client
	if isLocalConsensus {
		// go on a local blockchain
		cfgBlockchain, err := parseBlockchainConfig(cfg, log)
		if err != nil {
			return nil, fmt.Errorf("invalid blockchain configuration: %w", err)
		}

		if consensusAcc == nil {
			return nil, fmt.Errorf("missing account with label '%s' in wallet '%s'", consensusAccLabel, walletPass)
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

		localWSClient, err = server.bc.BuildWSClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("build WS client on internal blockchain: %w", err)
		}

		fsChainParams.key = server.key
		fsChainOpts := make([]client.Option, 3, 4)
		fsChainOpts[0] = client.WithContext(ctx)
		fsChainOpts[1] = client.WithLogger(log)
		fsChainOpts[2] = client.WithSingleClient(localWSClient)

		if !isAutoDeploy {
			fsChainOpts = append(fsChainOpts, client.WithAutoFSChainScope())
		}

		server.morphClient, err = client.New(server.key, fsChainOpts...)
		if err != nil {
			return nil, fmt.Errorf("init internal morph client: %w", err)
		}
	} else {
		if len(server.predefinedValidators) == 0 {
			return nil, fmt.Errorf("empty '%s' list in config", cfgPathFSChainValidators)
		}

		// fallback to the pure RPC architecture

		fsChainParams.key = server.key
		fsChainParams.withAutoFSChainScope = !isAutoDeploy

		server.morphClient, err = server.createClient(ctx, fsChainParams, errChan)
		if err != nil {
			return nil, err
		}
	}

	if isAutoDeploy {
		log.Info("auto-deployment configured, initializing FS chain...")

		var fschain *fsChain
		var clnt *client.Client // set if not isLocalConsensus only
		if isLocalConsensus {
			fschain = newFSChain(server.morphClient, localWSClient)
		} else {
			// create new client for deployment procedure only. This is done because event
			// subscriptions can be created only once, but we must cancel them to prevent
			// stuck
			//
			// connection switch/loose callbacks are not needed, so just create
			// another one-time client instead of server.createClient
			endpoints := cfg.GetStringSlice(cfgPathFSChainRPCEndpoints)
			if len(endpoints) == 0 {
				return nil, fmt.Errorf("configuration of FS chain RPC endpoints '%s' is missing or empty", cfgPathFSChainRPCEndpoints)
			}

			clnt, err = client.New(server.key,
				client.WithContext(ctx),
				client.WithLogger(log),
				client.WithDialTimeout(cfg.GetDuration(fsChainParams.name+".dial_timeout")),
				client.WithEndpoints(endpoints),
				client.WithReconnectionRetries(cfg.GetInt(fsChainParams.name+".reconnections_number")),
				client.WithReconnectionsDelay(cfg.GetDuration(fsChainParams.name+".reconnections_delay")),
				client.WithMinRequiredBlockHeight(fsChainParams.from),
			)
			if err != nil {
				return nil, fmt.Errorf("create multi-endpoint client for FS chain deployment: %w", err)
			}

			fschain = newFSChain(clnt, nil)
		}

		var deployPrm deploy.Prm
		deployPrm.Logger = server.log
		deployPrm.Blockchain = fschain
		deployPrm.LocalAccount = singleAcc
		deployPrm.ValidatorMultiSigAccount = consensusAcc
		deployPrm.Glagolitsa = &glagolitsa.Glagolitsa{}

		nnsCfg, err := parseNNSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("invalid NNS configuration: %w", err)
		}

		err = readEmbeddedContracts(&deployPrm)
		if err != nil {
			return nil, err
		}

		deployPrm.NNS.SystemEmail = nnsCfg.systemEmail
		if deployPrm.NNS.SystemEmail == "" {
			deployPrm.NNS.SystemEmail = "nonexistent@nspcc.io"
		}

		setNetworkSettingsDefaults(&deployPrm.NetmapContract.Config)

		server.setHealthStatus(control.HealthStatus_INITIALIZING_NETWORK)
		err = deploy.Deploy(ctx, deployPrm)
		if err != nil {
			return nil, fmt.Errorf("deploy FS chain: %w", err)
		}

		fschain.cancelSubs()
		if !isLocalConsensus {
			clnt.Close()
		}

		err = server.morphClient.InitFSChainScope()
		if err != nil {
			return nil, fmt.Errorf("init FS chain witness scope: %w", err)
		}

		server.log.Info("autodeploy completed")
	}

	// create morph listener
	server.morphListener, err = createListener(server.morphClient, fsChainParams)
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
		mainnetChain := fsChainParams
		mainnetChain.withAutoFSChainScope = false
		mainnetChain.name = mainnetPrefix

		fromMainChainBlock, err := server.persistate.UInt32(persistateMainChainLastBlockKey)
		if err != nil {
			fromMainChainBlock = 0
			log.Warn("can't get last processed main chain block number", zap.Error(err))
		}
		mainnetChain.from = fromMainChainBlock

		// create mainnet client
		server.mainnetClient, err = server.createClient(ctx, mainnetChain, errChan)
		if err != nil {
			return nil, err
		}

		// create mainnet listener
		server.mainnetListener, err = createListener(server.mainnetClient, mainnetChain)
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

	// enable notary support in the FS client
	err = server.morphClient.EnableNotarySupport(
		client.WithProxyContract(server.contracts.proxy),
	)
	if err != nil {
		return nil, fmt.Errorf("could not enable FS chain notary support: %w", err)
	}

	server.morphListener.EnableNotarySupport(server.contracts.proxy, server.key.PublicKey().GetScriptHash(),
		server.morphClient.Committee, server.morphClient)

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

	cnrClient, err := cntClient.NewFromMorph(server.morphClient, server.contracts.container, 0, cntClient.AsAlphabet())
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

	server.precision, err = server.balanceClient.Decimals()
	if err != nil {
		return nil, fmt.Errorf("can't read balance contract precision: %w", err)
	}

	reputationClient, err := repClient.NewFromMorph(server.morphClient, server.contracts.reputation, 0, repClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	neofsCli, err := neofsClient.NewFromMorph(server.mainnetClient, server.contracts.neofs,
		server.feeConfig.MainChainFee(), neofsClient.TryNotary(), neofsClient.AsAlphabet())
	if err != nil {
		return nil, err
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

	var buffers sync.Pool
	buffers.New = func() any {
		b := make([]byte, cache.DefaultBufferSize)
		return &b
	}

	clientCache := newClientCache(&clientCacheParams{
		Log:           log,
		Key:           &server.key.PrivateKey,
		SGTimeout:     cfg.GetDuration("audit.timeout.get"),
		HeadTimeout:   cfg.GetDuration("audit.timeout.head"),
		RangeTimeout:  cfg.GetDuration("audit.timeout.rangehash"),
		AllowExternal: cfg.GetBool("audit.allow_external"),
		Buffers:       &buffers,
	})

	server.registerNoErrCloser(clientCache.cache.CloseAll)

	pdpPoolSize := cfg.GetInt("audit.pdp.pairs_pool_size")
	porPoolSize := cfg.GetInt("audit.por.pool_size")

	// create audit processor dependencies
	server.auditTaskManager = audittask.New(
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

	server.workers = append(server.workers, server.auditTaskManager.Listen)

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
		TaskManager:      server.auditTaskManager,
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

	locodeValidator, err := server.newLocodeValidator()
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

	nnsContractAddr, err := server.morphClient.NNSHash()
	if err != nil {
		return nil, fmt.Errorf("get NeoFS NNS contract address: %w", err)
	}

	nnsService := newNeoFSNNS(nnsContractAddr, invoker.New(server.morphClient, nil))

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
			availabilityvalidator.New(),
			privatedomains.New(nnsService),
			locodeValidator,
		),
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
		NetworkState:    server.netmapClient,
		MetaEnabled:     cfg.GetBool("experimental.chain_meta_data"),
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(containerProcessor, server)
	if err != nil {
		return nil, err
	}

	precisionConverter := precision.NewConverter(server.precision)

	// create balance processor
	balanceProcessor, err := balance.New(&balance.Params{
		Log:           log,
		PoolSize:      cfg.GetInt("workers.balance"),
		NeoFSClient:   neofsCli,
		BalanceSC:     server.contracts.balance,
		AlphabetState: server,
		Converter:     precisionConverter,
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
			BalanceClient:       server.balanceClient,
			NetmapClient:        server.netmapClient,
			MorphClient:         server.morphClient,
			EpochState:          server,
			AlphabetState:       server,
			Converter:           precisionConverter,
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
		ReputationWrapper: reputationClient,
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

	return server, nil
}

func createListener(cli *client.Client, p chainParams) (event.Listener, error) {
	listener, err := event.NewListener(event.ListenerParams{
		Logger: p.log.With(zap.String("chain", p.name)),
		Client: cli,
	})
	if err != nil {
		return nil, err
	}

	return listener, err
}

func (s *Server) createClient(ctx context.Context, p chainParams, errChan chan<- error) (*client.Client, error) {
	endpoints := p.cfg.GetStringSlice(p.name + ".endpoints")
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%s chain client endpoints not provided", p.name)
	}
	var options = []client.Option{
		client.WithContext(ctx),
		client.WithLogger(p.log),
		client.WithDialTimeout(p.cfg.GetDuration(p.name + ".dial_timeout")),
		client.WithEndpoints(endpoints),
		client.WithReconnectionRetries(p.cfg.GetInt(p.name + ".reconnections_number")),
		client.WithReconnectionsDelay(p.cfg.GetDuration(p.name + ".reconnections_delay")),
		client.WithConnSwitchCallback(func() {
			var err error

			if p.name == fsChainPrefix || p.name == deprecatedMorphPrefix {
				err = s.restartMorph()
			} else {
				err = s.restartMainChain()
			}
			if err != nil {
				errChan <- fmt.Errorf("internal services' restart after RPC reconnection to the %s: %w", p.name, err)
			}
		}),
		client.WithMinRequiredBlockHeight(p.from),
	}
	if p.withAutoFSChainScope {
		options = append(options, client.WithAutoFSChainScope())
	}

	return client.New(p.key, options...)
}

func parsePredefinedValidators(cfg *viper.Viper) (keys.PublicKeys, error) {
	publicKeyStrings := cfg.GetStringSlice(cfgPathFSChainValidators)

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

	lastTick, err := s.netmapClient.LastEpochBlock()
	if err != nil {
		return fmt.Errorf("can't read last epoch block: %w", err)
	}

	blockHeight, err := s.morphClient.BlockCount()
	if err != nil {
		return fmt.Errorf("can't get FS chain height: %w", err)
	}

	// get next epoch delta tick
	delta := nextEpochBlockDelta(uint32(epochDuration), blockHeight, lastTick)

	s.epochCounter.Store(epoch)
	s.epochDuration.Store(epochDuration)
	s.initialEpochTickDelta.Store(delta)

	s.log.Info("read config from blockchain",
		zap.Bool("active", s.IsActive()),
		zap.Bool("alphabet", s.IsAlphabet()),
		zap.Uint64("epoch", epoch),
		zap.Uint32("precision", s.precision),
		zap.Uint32("last epoch tick block", lastTick),
		zap.Uint32("current chain height", blockHeight),
		zap.Uint32("next epoch tick after (blocks)", delta),
	)

	return nil
}

func nextEpochBlockDelta(duration, currentHeight, lastTick uint32) uint32 {
	delta := duration + lastTick
	if delta < currentHeight {
		return 0
	}

	return delta - currentHeight
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

func (s *Server) restartMorph() error {
	s.log.Info("restarting internal services because of RPC connection loss...")

	s.auditTaskManager.Reset()
	s.statusIndex.reset()

	err := s.initConfigFromBlockchain()
	if err != nil {
		return fmt.Errorf("FS chain config reinitialization: %w", err)
	}

	if err = s.epochTimer.Reset(); err != nil {
		return fmt.Errorf("could not reset new epoch block timer: %w", err)
	}

	s.log.Info("internal services have been restarted after RPC connection loss...")

	return nil
}

func (s *Server) restartMainChain() error {
	return nil
}

func serveControl(server *Server, log *zap.Logger, cfg *viper.Viper, errChan chan<- error) error {
	controlSvcEndpoint := cfg.GetString("control.grpc.endpoint")
	if controlSvcEndpoint != "" {
		authKeysStr := cfg.GetStringSlice("control.authorized_keys")
		authKeys := make([][]byte, 0, len(authKeysStr))

		for i := range authKeysStr {
			key, err := hex.DecodeString(authKeysStr[i])
			if err != nil {
				return fmt.Errorf("could not parse Control authorized key %s: %w",
					authKeysStr[i],
					err,
				)
			}

			authKeys = append(authKeys, key)
		}

		lis, err := net.Listen("tcp", controlSvcEndpoint)
		if err != nil {
			return err
		}
		var p controlsrv.Prm

		p.SetPrivateKey(*server.key)
		p.SetHealthChecker(server)
		p.SetNetworkManager(server)

		controlSvc := controlsrv.New(p,
			controlsrv.WithAllowedKeys(authKeys),
		)

		grpcControlSrv := grpc.NewServer()
		control.RegisterControlServiceServer(grpcControlSrv, controlSvc)

		go func() {
			errChan <- grpcControlSrv.Serve(lis)
		}()

		server.registerNoErrCloser(grpcControlSrv.GracefulStop)
	} else {
		log.Info("no Control server endpoint specified, service is disabled")
	}

	return nil
}

func serveMetrics(server *Server, cfg *viper.Viper) {
	if cfg.GetString("prometheus.address") != "" {
		m := metrics.NewInnerRingMetrics(misc.Version)
		server.metrics = &m
	}
}
