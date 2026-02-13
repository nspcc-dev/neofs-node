package innerring

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"path"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-contract/deploy"
	"github.com/nspcc-dev/neofs-node/internal/chaintime"
	"github.com/nspcc-dev/neofs-node/misc"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/balance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/container"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	nodevalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation"
	availabilityvalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/availability"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/external"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/privatedomains"
	statevalidation "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/state"
	addrvalidator "github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/structure"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	"github.com/nspcc-dev/neofs-node/pkg/metrics"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	balanceClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	neofsClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	repClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	controlsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	reputationcommon "github.com/nspcc-dev/neofs-node/pkg/services/reputation/common"
	"github.com/nspcc-dev/neofs-node/pkg/timers"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	// Server is the inner ring application structure that contains all event
	// processors, shared variables and event handlers.
	Server struct {
		log *zap.Logger

		bc        *blockchain.Blockchain
		metaChain *blockchain.Blockchain

		// event producers
		fsChainListener event.Listener
		mainnetListener event.Listener
		epochTimers     *timers.EpochTimers

		fsChainClient *client.Client
		mainnetClient *client.Client
		balanceClient *balanceClient.Client
		netmapClient  *nmClient.Client

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
		mainNotaryConfig *notaryConfig

		// internal variables
		key                  *keys.PrivateKey
		pubKey               []byte
		contracts            *contracts
		predefinedValidators keys.PublicKeys
		withoutMainNet       bool
		chainTime            chaintime.AtomicChainTimeProvider

		// runtime processors
		netmapProcessor    *netmap.Processor
		containerProcessor *container.Processor

		workers []func(context.Context) error

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
		cfg  *config.BasicChain
		key  *keys.PrivateKey
		name string
		from uint32 // block height

		withAutoFSChainScope bool
	}
)

const (
	mainnetPrefix = "mainnet"

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
		err = s.depositMainNotary(ctx)
		if err != nil {
			return fmt.Errorf("main notary deposit: %w", err)
		}

		s.log.Info("made main chain notary deposit successfully")
	}

	err = s.depositFSNotary(ctx)
	if err != nil {
		return fmt.Errorf("fs chain notary deposit: %w", err)
	}

	s.log.Info("made fs chain notary deposit successfully")

	// vote for FS chain validator if it is prepared in config
	err = s.voteForFSChainValidator(ctx, s.predefinedValidators, nil)
	if err != nil {
		// we don't stop inner ring execution on this error
		s.log.Warn("can't vote for prepared validators",
			zap.Error(err))
	}

	fsChainErr := make(chan error)
	mainnnetErr := make(chan error)

	// anonymous function to multiplex error channels
	go func() {
		select {
		case <-ctx.Done():
			return
		case err := <-fsChainErr:
			intError <- fmt.Errorf("FS chain: %w", err)
		case err := <-mainnnetErr:
			intError <- fmt.Errorf("mainnet: %w", err)
		}
	}()

	s.fsChainListener.RegisterHeaderHandler(func(b *block.Header) {
		s.log.Debug("new block",
			zap.Uint32("index", b.Index),
		)

		s.epochTimers.UpdateTime(b.Timestamp)
		s.chainTime.Set(b.Timestamp)

		err = s.persistate.SetUInt32(persistateFSChainLastBlockKey, b.Index)
		if err != nil {
			s.log.Warn("can't update persistent state",
				zap.String("chain", "FS"),
				zap.Uint32("block_index", b.Index))
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

	go s.fsChainListener.ListenWithError(ctx, fsChainErr)  // listen for neo:fs events
	go s.mainnetListener.ListenWithError(ctx, mainnnetErr) // listen for neo:mainnet events

	if s.IsAlphabet() {
		go func() {
			if err := s.containerProcessor.AddContainerStructs(ctx); err != nil {
				fsChainErr <- fmt.Errorf("structurize containers in the contract: %w", err)
			}
		}()
	}

	s.startWorkers(ctx)

	return nil
}

// must be called with a zero value if last tick block is unknown. Returns
// next tick's timestamp accurate to milliseconds.
func (s *Server) resetEpochTimer(lastTickHeight uint32) (uint64, error) {
	epochDuration, err := s.netmapClient.EpochDuration()
	if err != nil {
		return 0, fmt.Errorf("can't read epoch duration: %w", err)
	}

	lastTick := lastTickHeight
	if lastTick == 0 {
		lastTick, err = s.netmapClient.LastEpochBlock()
		if err != nil {
			return 0, fmt.Errorf("can't read last epoch's block: %w", err)
		}
	}
	lastTickH, err := s.fsChainClient.GetBlockHeader(lastTick)
	if err != nil {
		return 0, fmt.Errorf("can't read last tick's block header (#%d): %w", lastTick, err)
	}

	const msInS = 1000
	s.epochTimers.Reset(lastTickH.Timestamp, epochDuration*msInS)

	return lastTickH.Timestamp + epochDuration*msInS, nil
}

func (s *Server) startWorkers(ctx context.Context) {
	for _, w := range s.workers {
		go w(ctx)
	}
}

// Stop closes all subscription channels.
func (s *Server) Stop() {
	s.setHealthStatus(control.HealthStatus_SHUTTING_DOWN)

	go s.fsChainListener.Stop()
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
func New(ctx context.Context, log *zap.Logger, cfg *config.Config, errChan chan<- error) (*Server, error) {
	// Never shadow this var, we have defers relying on it.
	var err error
	server := &Server{log: log}

	server.setHealthStatus(control.HealthStatus_HEALTH_STATUS_UNDEFINED)

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

	fsChainParams := chainParams{
		log:  log,
		cfg:  &cfg.FSChain.BasicChain,
		name: cfgFSChainName,
		from: fromFSChainBlock,
	}

	const walletPathKey = "wallet.path"
	if cfg.Wallet.Path == "" {
		return nil, fmt.Errorf("file path to the node Neo wallet is not configured '%s'", walletPathKey)
	}

	walletPath := cfg.Wallet.Path
	walletPass := cfg.Wallet.Password

	// parse default validators
	server.predefinedValidators = cfg.FSChain.Validators
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
	var serverAcc *wallet.Account
	var consensusAcc *wallet.Account

	for i := range wlt.Accounts {
		err = wlt.Accounts[i].Decrypt(walletPass, wlt.Scrypt)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt account '%s' (%s) in wallet '%s': %w", wlt.Accounts[i].Label, cfg.Wallet.Address, walletPath, err)
		}
		if wlt.Accounts[i].Address == cfg.Wallet.Address {
			serverAcc = wlt.Accounts[i]
			if singleAcc == nil {
				singleAcc = serverAcc
			}
		}

		switch wlt.Accounts[i].Label {
		case singleAccLabel:
			singleAcc = wlt.Accounts[i]
			if serverAcc == nil {
				serverAcc = singleAcc
			}
		case consensusAccLabel:
			consensusAcc = wlt.Accounts[i]
		}
	}

	if serverAcc == nil {
		return nil, fmt.Errorf("missing server private key in wallet '%s'", walletPath)
	}
	server.key = serverAcc.PrivateKey()

	isLocalConsensus, err := isLocalConsensusMode(cfg)
	if err != nil {
		return nil, fmt.Errorf("invalid consensus configuration: %w", err)
	}

	err = serveControl(server, log, cfg, errChan)
	if err != nil {
		return nil, err
	}

	serveMetrics(server, cfg)

	var localWSClient *rpcclient.WSClient // set if isLocalConsensus only

	// create FS chain client
	if isLocalConsensus {
		// go on a local blockchain
		err = validateBlockchainConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("invalid blockchain configuration: %w", err)
		}

		if consensusAcc == nil {
			return nil, fmt.Errorf("missing account with label '%s' in wallet '%s'", consensusAccLabel, walletPath)
		}

		if len(server.predefinedValidators) == 0 {
			server.predefinedValidators = cfg.FSChain.Consensus.Committee
		}

		server.bc, err = blockchain.New(&cfg.FSChain.Consensus, &cfg.Wallet, errChan, log)
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

			// It's critical for err to not be shadowed, otherwise
			// blockchain won't be stopped gracefully on error.
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

		if cfg.FSChain.DisableAutodeploy {
			fsChainOpts = append(fsChainOpts, client.WithAutoFSChainScope())
		}

		server.fsChainClient, err = client.New(server.key, fsChainOpts...)
		if err != nil {
			return nil, fmt.Errorf("init internal FS chain client: %w", err)
		}
	} else {
		if len(server.predefinedValidators) == 0 {
			return nil, fmt.Errorf("empty '%s' list in config", cfgFSChainName+".validators")
		}

		// fallback to the pure RPC architecture

		fsChainParams.key = server.key
		fsChainParams.withAutoFSChainScope = cfg.FSChain.DisableAutodeploy

		server.fsChainClient, err = server.createClient(ctx, fsChainParams, errChan)
		if err != nil {
			return nil, err
		}
	}

	if !cfg.FSChain.DisableAutodeploy {
		log.Info("auto-deployment configured, initializing FS chain...")

		var fschain *fsChain
		var clnt *client.Client // set if not isLocalConsensus only
		if isLocalConsensus {
			fschain = newFSChain(server.fsChainClient, localWSClient)
		} else {
			// create new client for deployment procedure only. This is done because event
			// subscriptions can be created only once, but we must cancel them to prevent
			// stuck
			//
			// connection switch/loose callbacks are not needed, so just create
			// another one-time client instead of server.createClient
			if len(cfg.FSChain.Endpoints) == 0 {
				return nil, fmt.Errorf("configuration of FS chain RPC endpoints '%s' is missing or empty", cfgFSChainName+".endpoints")
			}

			clnt, err = client.New(server.key,
				client.WithContext(ctx),
				client.WithLogger(log),
				client.WithDialTimeout(cfg.FSChain.DialTimeout),
				client.WithEndpoints(cfg.FSChain.Endpoints),
				client.WithReconnectionRetries(cfg.FSChain.ReconnectionsNumber),
				client.WithReconnectionsDelay(cfg.FSChain.ReconnectionsDelay),
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

		err = readEmbeddedContracts(&deployPrm)
		if err != nil {
			return nil, err
		}

		deployPrm.NNS.SystemEmail = cfg.NNS.SystemEmail
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

		err = server.fsChainClient.InitFSChainScope()
		if err != nil {
			return nil, fmt.Errorf("init FS chain witness scope: %w", err)
		}

		server.log.Info("autodeploy completed")
	}

	// create fs chain listener
	server.fsChainListener, err = createListener(server.fsChainClient, fsChainParams)
	if err != nil {
		return nil, err
	}

	server.withoutMainNet = !cfg.Mainnet.Enabled

	if server.withoutMainNet {
		// This works as long as event Listener starts listening loop once,
		// otherwise Server.Start will run two similar routines.
		// This behavior most likely will not change.
		server.mainnetListener = server.fsChainListener
		server.mainnetClient = server.fsChainClient
	} else {
		mainnetChain := fsChainParams
		mainnetChain.withAutoFSChainScope = false
		mainnetChain.name = mainnetPrefix
		mainnetChain.cfg = &cfg.Mainnet.BasicChain

		mainnetChain.from, err = server.persistate.UInt32(persistateMainChainLastBlockKey)
		if err != nil {
			log.Warn("can't get last processed main chain block number", zap.Error(err))
		}

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
		&cfg.Mainnet.Contracts,
		server.fsChainClient,
		server.withoutMainNet,
		server.mainNotaryConfig.disabled,
	)
	if err != nil {
		return nil, err
	}

	// enable notary support in the FS client
	err = server.fsChainClient.EnableNotarySupport(
		client.WithProxyContract(server.contracts.proxy),
	)
	if err != nil {
		return nil, fmt.Errorf("could not enable FS chain notary support: %w", err)
	}

	server.fsChainListener.EnableNotarySupport(server.contracts.proxy, server.key.PublicKey().GetScriptHash(),
		server.fsChainClient.Committee, server.fsChainClient)

	if !server.mainNotaryConfig.disabled {
		// enable notary support in the main client
		err = server.mainnetClient.EnableNotarySupport(
			client.WithProxyContract(server.contracts.processing),
			client.WithAlphabetSource(server.fsChainClient.Committee),
		)
		if err != nil {
			return nil, fmt.Errorf("could not enable main chain notary support: %w", err)
		}
	}

	server.pubKey = server.key.PublicKey().Bytes()

	cnrClient, err := cntClient.NewFromMorph(server.fsChainClient, server.contracts.container, cntClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	server.netmapClient, err = nmClient.NewFromMorph(server.fsChainClient, server.contracts.netmap, nmClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	server.balanceClient, err = balanceClient.NewFromMorph(server.fsChainClient, server.contracts.balance, balanceClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	server.precision, err = server.balanceClient.Decimals()
	if err != nil {
		return nil, fmt.Errorf("can't read balance contract precision: %w", err)
	}

	reputationClient, err := repClient.NewFromMorph(server.fsChainClient, server.contracts.reputation, repClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	neofsCli, err := neofsClient.NewFromMorph(server.mainnetClient, server.contracts.neofs,
		fixedn.Fixed8(cfg.Mainnet.ExtraFee), neofsClient.TryNotary(), neofsClient.AsAlphabet())
	if err != nil {
		return nil, err
	}

	var irf = NewIRFetcherWithNotary(server.fsChainClient)

	server.statusIndex = newInnerRingIndexer(
		server.fsChainClient,
		irf,
		server.key.PublicKey(),
		cfg.Indexer.CacheTimeout,
	)

	// create settlement processor
	settlementProcessor := settlement.New(
		settlement.Prm{
			State:           server,
			ContainerClient: cnrClient,
			NetmapClient:    server.netmapClient,
			BalanceClient:   server.balanceClient,
		},
		settlement.WithLogger(server.log.With(zap.String("component", "basicIncomeProcessor"))),
	)

	locodeValidator, err := server.newLocodeValidator()
	if err != nil {
		return nil, err
	}

	var alphaSync event.Handler

	if server.withoutMainNet || cfg.Mainnet.DisableGovernanceSync {
		alphaSync = func(event.Event) {
			log.Debug("alphabet keys sync is disabled")
		}
	} else {
		var governanceProcessor *governance.Processor
		// create governance processor
		governanceProcessor, err = governance.New(&governance.Params{
			Log:           log,
			NeoFSClient:   neofsCli,
			NetmapClient:  server.netmapClient,
			AlphabetState: server,
			EpochState:    server,
			Voter:         server,
			IRFetcher:     irf,
			FSChainClient: server.fsChainClient,
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

	nnsContractAddr, err := server.fsChainClient.NNSHash()
	if err != nil {
		return nil, fmt.Errorf("get NeoFS NNS contract address: %w", err)
	}

	nnsService := newNeoFSNNS(nnsContractAddr, invoker.New(server.fsChainClient, nil))

	nodeValidators := []netmap.NodeValidator{
		statevalidation.New(),
		addrvalidator.New(),
		availabilityvalidator.New(),
		privatedomains.New(nnsService),
		locodeValidator,
	}
	if cfg.Validator.Enabled && cfg.Validator.URL != "" {
		nodeValidators = append(nodeValidators, external.New(cfg.Validator.URL, server.key))
	}

	var metaActor *notary.Actor
	if cfg.Experimental.ChainMetaData {
		if !isLocalConsensus {
			return nil, errors.New("experimental meta-on-chain is not supported for non-consensus Alphabet nodes")
		}

		v, err := server.fsChainClient.GetVersion()
		if err != nil {
			return nil, fmt.Errorf("fetchin FS chain version: %w", err)
		}

		metaSeeds, err := incPort(cfg.FSChain.Consensus.SeedNodes)
		if err != nil {
			return nil, fmt.Errorf("parsing consensus seed nodes: %w", err)
		}
		metaRPCs, err := incPort(cfg.FSChain.Consensus.RPC.Listen)
		if err != nil {
			return nil, fmt.Errorf("parsing consensus RPCs: %w", err)
		}
		metaRPCsTLS, err := incPort(cfg.FSChain.Consensus.RPC.TLS.Listen)
		if err != nil {
			return nil, fmt.Errorf("parsing consensus RPCs (TLS): %w", err)
		}
		metaP2Ps, err := incPort(cfg.FSChain.Consensus.P2P.Listen)
		if err != nil {
			return nil, fmt.Errorf("parsing consensus P2Ps: %w", err)
		}

		fsChainProtocol := v.Protocol
		metaChainCfg := config.Consensus{
			Storage: config.Storage{
				Path: path.Join(path.Dir(cfg.FSChain.Consensus.Storage.Path), "meta_db.bolt"),
				Type: dbconfig.BoltDB,
			},
			SeedNodes: metaSeeds,
			RPC: config.RPC{
				Listen:              metaRPCs,
				MaxWebSocketClients: cfg.FSChain.Consensus.RPC.MaxWebSocketClients,
				SessionPoolSize:     cfg.FSChain.Consensus.RPC.SessionPoolSize,
				MaxGasInvoke:        cfg.FSChain.Consensus.RPC.MaxGasInvoke,
				TLS: config.TLS{
					Enabled:  cfg.FSChain.Consensus.RPC.TLS.Enabled,
					Listen:   metaRPCsTLS,
					CertFile: cfg.FSChain.Consensus.RPC.TLS.CertFile,
					KeyFile:  cfg.FSChain.Consensus.RPC.TLS.KeyFile,
				},
			},
			P2P: config.P2P{
				DialTimeout:       cfg.FSChain.Consensus.P2P.DialTimeout,
				ProtoTickInterval: cfg.FSChain.Consensus.P2P.ProtoTickInterval,
				Listen:            metaP2Ps,
				Peers:             cfg.FSChain.Consensus.P2P.Peers,
				Ping:              cfg.FSChain.Consensus.P2P.Ping,
			},
			MaxTimePerBlock: cfg.FSChain.Consensus.MaxTimePerBlock,

			Magic:                       uint32(fsChainProtocol.Network) + 1,
			Committee:                   fsChainProtocol.StandbyCommittee,
			TimePerBlock:                time.Duration(fsChainProtocol.MillisecondsPerBlock) * time.Millisecond,
			MaxTraceableBlocks:          fsChainProtocol.MaxTraceableBlocks,
			MaxValidUntilBlockIncrement: fsChainProtocol.MaxValidUntilBlockIncrement,

			Hardforks:                       config.Hardforks{},
			ValidatorsHistory:               config.ValidatorsHistory{},
			SetRolesInGenesis:               true,
			KeepOnlyLatestState:             false,
			RemoveUntraceableBlocks:         false,
			P2PNotaryRequestPayloadPoolSize: 1000, // default for blockchain.New()
		}

		server.metaChain, err = metachain.NewMetaChain(&metaChainCfg, &cfg.Wallet, errChan, log.With(zap.String("component", "metadata chain")))
		if err != nil {
			return nil, fmt.Errorf("init meta sidechain blockchain: %w", err)
		}
		server.workers = append(server.workers, server.metaChain.Run)

		alphabetList, err := server.fsChainClient.NeoFSAlphabetList()
		if err != nil {
			return nil, fmt.Errorf("fetching FS chain Alphabet: %w", err)
		}
		metaCli, err := server.metaChain.BuildWSClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("build meta chain client: %w", err)
		}
		alphaAcc := wallet.NewAccountFromPrivateKey(server.key)
		err = alphaAcc.ConvertMultisig(sc.GetMajorityHonestNodeCount(len(alphabetList)), alphabetList)
		if err != nil {
			return nil, fmt.Errorf("build meta committee acc: %w", err)
		}
		metaActor, err = notary.NewActor(metaCli, []actor.SignerAccount{
			{
				Signer: transaction.Signer{
					Account: alphaAcc.ScriptHash(),
					Scopes:  transaction.CalledByEntry,
				},
				Account: alphaAcc,
			},
		}, wallet.NewAccountFromPrivateKey(server.key))
		if err != nil {
			return nil, fmt.Errorf("build meta committee actor: %w", err)
		}

		server.workers = append(server.workers, func(ctx context.Context) error {
			simpleAcc := wallet.NewAccountFromPrivateKey(server.key)
			simpleAccHash := simpleAcc.ScriptHash()
			act, err := actor.New(metaCli, []actor.SignerAccount{{
				Signer: transaction.Signer{
					Account: simpleAccHash,
					Scopes:  transaction.CalledByEntry,
				},
				Account: simpleAcc,
			}})

			gasAct := gas.New(act)
			txHash, vub, err := gasAct.Transfer(
				simpleAccHash,
				notary.Hash,
				big.NewInt(90*native.GASFactor), // default meta chain balance but a little bit less
				&notary.OnNEP17PaymentData{Account: &simpleAccHash, Till: math.MaxUint32})
			if err != nil {
				if !errors.Is(err, neorpc.ErrAlreadyExists) {
					return fmt.Errorf("can't make notary deposit in meta chain: %w", err)
				}
			}

			server.log.Debug("made meta chain notary deposit, awaiting...", zap.String("txHash", txHash.StringLE()), zap.Uint32("vub", vub))

			_, err = act.WaitSuccess(ctx, txHash, vub, nil)
			if err != nil {
				return fmt.Errorf("waiting for meta chain notary deposit %s TX to be persisted: %w", txHash.StringLE(), err)
			}

			server.log.Debug("meta chain notary deposit successful", zap.String("tx_hash", txHash.StringLE()))

			return nil
		})
	}

	// create netmap processor
	server.netmapProcessor, err = netmap.New(&netmap.Params{
		Log:              log,
		PoolSize:         cfg.Workers.Netmap,
		MetaClient:       metaActor,
		NetmapClient:     server.netmapClient,
		EpochTimer:       server,
		EpochState:       server,
		AlphabetState:    server,
		ContainerWrapper: cnrClient,
		NotaryDepositHandler: server.onlyAlphabetEventHandler(
			server.notaryHandler,
		),
		AlphabetSyncHandler: alphaSync,
		NodeValidator:       nodevalidator.New(nodeValidators...),
	})
	if err != nil {
		return nil, err
	}

	err = bindFSChainProcessor(server.netmapProcessor, server)
	if err != nil {
		return nil, err
	}

	// container processor
	server.containerProcessor, err = container.New(&container.Params{
		Log:             log,
		PoolSize:        cfg.Workers.Container,
		AlphabetState:   server,
		ContainerClient: cnrClient,
		MetaClient:      metaActor,
		NetworkState:    server.netmapClient,
		MetaEnabled:     cfg.Experimental.ChainMetaData,
		AllowEC:         cfg.Experimental.AllowEC,
		ChainTime:       &server.chainTime,
	})
	if err != nil {
		return nil, err
	}

	err = bindFSChainProcessor(server.containerProcessor, server)
	if err != nil {
		return nil, err
	}

	precisionConverter := precision.NewConverter(server.precision)

	// create balance processor
	balanceProcessor, err := balance.New(&balance.Params{
		Log:           log,
		PoolSize:      cfg.Workers.Balance,
		NeoFSClient:   neofsCli,
		BalanceSC:     server.contracts.balance,
		AlphabetState: server,
		Converter:     precisionConverter,
	})
	if err != nil {
		return nil, err
	}

	err = bindFSChainProcessor(balanceProcessor, server)
	if err != nil {
		return nil, err
	}

	if !server.withoutMainNet {
		var neofsProcessor *neofs.Processor
		// create mainnnet neofs processor
		neofsProcessor, err = neofs.New(&neofs.Params{
			Log:                 log,
			PoolSize:            cfg.Workers.NeoFS,
			NeoFSContract:       server.contracts.neofs,
			BalanceClient:       server.balanceClient,
			NetmapClient:        server.netmapClient,
			FSChainClient:       server.fsChainClient,
			EpochState:          server,
			AlphabetState:       server,
			Converter:           precisionConverter,
			MintEmitCacheSize:   cfg.Emit.Mint.CacheSize,
			MintEmitThreshold:   cfg.Emit.Mint.Threshold,
			MintEmitValue:       fixedn.Fixed8(cfg.Emit.Mint.Value),
			GasBalanceThreshold: cfg.Emit.Gas.BalanceThreshold,
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
		PoolSize:          cfg.Workers.Alphabet,
		AlphabetContracts: server.contracts.alphabet,
		NetmapClient:      server.netmapClient,
		FSChainClient:     server.fsChainClient,
		IRList:            server,
		StorageEmission:   cfg.Emit.Storage.Amount,
	})
	if err != nil {
		return nil, err
	}

	err = bindFSChainProcessor(alphabetProcessor, server)
	if err != nil {
		return nil, err
	}

	// create reputation processor
	reputationProcessor, err := reputation.New(&reputation.Params{
		Log:               log,
		PoolSize:          cfg.Workers.Reputation,
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

	err = bindFSChainProcessor(reputationProcessor, server)
	if err != nil {
		return nil, err
	}

	initTimers(server, cfg, settlementProcessor)

	return server, nil
}

func incPort(addrs []string) ([]string, error) {
	res := slices.Clone(addrs)
	for i := range res {
		host, port, err := net.SplitHostPort(res[i])
		if err != nil {
			return nil, fmt.Errorf("[%d] address ('%s') cannot be parsed: %w", i, res[i], err)
		}
		portU, err := strconv.ParseUint(port, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("[%d] address ('%s') cannot be parsed: %w", i, port, err)
		}
		portU++
		res[i] = net.JoinHostPort(host, strconv.FormatUint(portU, 10))
	}

	return res, nil
}

func initTimers(server *Server, cfg *config.Config, paymentProcessor *settlement.Processor) {
	basicIncomeTick, stopBasicIncomeFunc := util.SingleAsyncExecutingInstance(func() {
		epochN := server.EpochCounter()
		if epochN == 0 { // estimates are invalid in genesis epoch
			return
		}

		paymentProcessor.HandleBasicIncomeEvent(settlement.NewBasicIncomeEvent(epochN - 1))
	})
	server.closers = append(server.closers, func() error { stopBasicIncomeFunc(); return nil })

	epochH, stopEpochHandlerFunc := util.SingleAsyncExecutingInstance(server.netmapProcessor.HandleNewEpochTick)
	server.closers = append(server.closers, func() error { stopEpochHandlerFunc(); return nil })

	ticks := timers.EpochTicks{
		NewEpochTicks: []timers.Tick{epochH},
		DeltaTicks: []timers.SubEpochTick{
			{
				Tick:     basicIncomeTick,
				EpochMul: cfg.Timers.CollectBasicIncome.Mul,
				EpochDiv: cfg.Timers.CollectBasicIncome.Div,
			},
		},
	}

	server.epochTimers = timers.NewTimers(ticks)
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
	endpoints := p.cfg.Endpoints
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%s chain client endpoints not provided", p.name)
	}
	var options = []client.Option{
		client.WithContext(ctx),
		client.WithLogger(p.log),
		client.WithDialTimeout(p.cfg.DialTimeout),
		client.WithEndpoints(endpoints),
		client.WithReconnectionRetries(p.cfg.ReconnectionsNumber),
		client.WithReconnectionsDelay(p.cfg.ReconnectionsDelay),
		client.WithConnSwitchCallback(func() {
			var err error

			if p.name == cfgFSChainName {
				err = s.restartFSChain()
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

	blockHeight, err := s.fsChainClient.BlockCount()
	if err != nil {
		return fmt.Errorf("can't get FS chain height: %w", err)
	}

	nextTickAt, err := s.resetEpochTimer(0)
	if err != nil {
		return fmt.Errorf("could not reset epoch timer: %w", err)
	}

	s.epochCounter.Store(epoch)
	s.epochDuration.Store(epochDuration)

	s.log.Info("read config from blockchain",
		zap.Bool("active", s.IsActive()),
		zap.Bool("alphabet", s.IsAlphabet()),
		zap.Uint64("epoch", epoch),
		zap.Uint32("precision", s.precision),
		zap.Uint32("last epoch tick block", lastTick),
		zap.Uint32("current chain height", blockHeight),
		zap.Uint64("next epoch tick at (timestamp, ms)", nextTickAt),
	)

	return nil
}

// onlyAlphabetEventHandler wrapper around event handler that executes it
// only if inner ring node is alphabet node.
func (s *Server) onlyAlphabetEventHandler(f event.Handler) event.Handler {
	return func(ev event.Event) {
		if s.IsAlphabet() {
			f(ev)
		}
	}
}

func (s *Server) restartFSChain() error {
	s.log.Info("restarting internal services because of RPC connection loss...")

	s.statusIndex.reset()

	err := s.initConfigFromBlockchain()
	if err != nil {
		return fmt.Errorf("FS chain config reinitialization: %w", err)
	}

	s.log.Info("internal services have been restarted after RPC connection loss...")

	return nil
}

func (s *Server) restartMainChain() error {
	return nil
}

func serveControl(server *Server, log *zap.Logger, cfg *config.Config, errChan chan<- error) error {
	controlSvcEndpoint := cfg.Control.GRPC.Endpoint
	if controlSvcEndpoint != "" {
		authKeys := make([][]byte, 0, len(cfg.Control.AuthorizedKeys))

		for _, v := range cfg.Control.AuthorizedKeys {
			authKeys = append(authKeys, v.Bytes())
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

func serveMetrics(server *Server, cfg *config.Config) {
	if cfg.Prometheus.Address != "" {
		m := metrics.NewInnerRingMetrics(misc.Version)
		server.metrics = &m
	}
}
