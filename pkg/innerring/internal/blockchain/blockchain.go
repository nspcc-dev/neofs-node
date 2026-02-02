package blockchain

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	neogoconfig "github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/consensus"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/network"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/services/notary"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"go.uber.org/zap"
)

// Blockchain provides Neo blockchain services consumed by NeoFS Inner Ring
// (hereinafter node). By design, Blockchain does not implement node specifics:
// instead, it provides the generic functionality of the Neo blockchain, and
// narrows the rich Neo functionality to the minimum necessary for the node's
// operation.
//
// Blockchain must be initialized using New constructor. After initialization
// Blockchain becomes a single-use component that can be started and then
// stopped. All operations should be executed after Blockchain is started and
// before it is stopped (any other behavior is undefined).
type Blockchain struct {
	logger    *zap.Logger
	storage   storage.Store
	core      *core.Blockchain
	netServer *network.Server
	rpcServer *rpcsrv.Server

	chErr chan error
}

// New returns new Blockchain configured by the specified Config. New panics if
// any required Config field is zero or unset. Resulting Blockchain is ready to
// run. Launched Blockchain should be finally stopped.
func New(cfg *config.Consensus, wallet *config.Wallet, errChan chan<- error, log *zap.Logger, customNatives ...func(cfg neogoconfig.ProtocolConfiguration) []interop.Contract) (res *Blockchain, err error) {
	switch {
	case cfg.Storage.Type == "":
		panic("uninitialized storage config")
	case cfg.TimePerBlock < 0:
		panic("negative block interval")
	case cfg.MaxTimePerBlock < 0:
		panic("negative max block interval")
	case wallet.Path == "":
		panic("missing wallet path")
	case errChan == nil:
		panic("missing error channel")
	case cfg.Magic == 0:
		// actually zero magic is valid, but almost definitely forgotten
		panic("missing network magic")
	case len(cfg.Committee) == 0:
		panic("empty committee")
	case cfg.P2P.Peers.Min > math.MaxInt32:
		panic(fmt.Sprintf("min peers is out of allowable range %d", cfg.P2P.Peers.Min))
	case cfg.P2P.Peers.Max > math.MaxInt32:
		panic(fmt.Sprintf("max peers is out of allowable range %d", cfg.P2P.Peers.Max))
	case cfg.P2P.Peers.Attempts > math.MaxInt32:
		panic(fmt.Sprintf("connection attempts number is out of allowable range %d", cfg.P2P.Peers.Attempts))
	case cfg.P2P.Ping.Interval < 0:
		panic("negative ping interval")
	case cfg.P2P.Ping.Timeout < 0:
		panic("negative ping timeout")
	case cfg.P2P.DialTimeout < 0:
		panic("negative dial timeout")
	case cfg.P2P.ProtoTickInterval < 0:
		panic("negative proto tick interval")
	}

	if cfg.ValidatorsHistory.Height != nil {
		_, ok := cfg.ValidatorsHistory.Height[0]
		if !ok {
			panic("missing 0 (genesis) height in validators history")
		}

		committeeSize := uint32(cfg.Committee.Len())

		for height, num := range cfg.ValidatorsHistory.Height {
			if height%committeeSize != 0 {
				panic(fmt.Sprintf("validators history's height is not a multiple of the committee size: %d%%%d != 0", height, committeeSize))
			}
			if num == 0 {
				panic(fmt.Sprintf("zero number of validators at height %d", height))
			}
			if num > committeeSize {
				panic(fmt.Sprintf("number of validators at height %d exceeds committee: %d > %d", height, num, committeeSize))
			}
		}
	}

	if log == nil {
		log = zap.NewNop()
	}
	if cfg.TimePerBlock == 0 {
		cfg.TimePerBlock = 15 * time.Second
	}
	if cfg.P2P.Peers.Max == 0 {
		cfg.P2P.Peers.Max = 100
	}
	if cfg.P2P.Peers.Attempts == 0 {
		cfg.P2P.Peers.Attempts = cfg.P2P.Peers.Min + 10
	}
	if cfg.P2P.DialTimeout == 0 {
		cfg.P2P.DialTimeout = time.Minute
	}
	if cfg.P2P.ProtoTickInterval == 0 {
		cfg.P2P.ProtoTickInterval = 2 * time.Second
	}
	if cfg.MaxTraceableBlocks == 0 {
		cfg.MaxTraceableBlocks = 17280
	}
	if cfg.MaxValidUntilBlockIncrement == 0 {
		cfg.MaxValidUntilBlockIncrement = 8640
	}
	if cfg.P2P.Ping.Interval == 0 {
		cfg.P2P.Ping.Interval = 30 * time.Second
	}
	if cfg.P2P.Ping.Timeout == 0 {
		cfg.P2P.Ping.Timeout = time.Minute
	}
	if cfg.RPC.MaxWebSocketClients == 0 {
		cfg.RPC.MaxWebSocketClients = 64
	}
	if cfg.RPC.SessionPoolSize == 0 {
		cfg.RPC.SessionPoolSize = 20
	}
	if cfg.P2PNotaryRequestPayloadPoolSize == 0 {
		cfg.P2PNotaryRequestPayloadPoolSize = 1000
	}
	if cfg.RPC.MaxGasInvoke == 0 {
		cfg.RPC.MaxGasInvoke = 100
	}

	standByCommittee := make([]string, len(cfg.Committee))
	for i := range cfg.Committee {
		standByCommittee[i] = cfg.Committee[i].StringCompressed()
	}

	var cfgBase neogoconfig.Config

	cfgBaseProto := &cfgBase.ProtocolConfiguration
	cfgBaseProto.Magic = netmode.Magic(cfg.Magic)
	cfgBaseProto.StandbyCommittee = standByCommittee

	cfgBaseProto.TimePerBlock = cfg.TimePerBlock
	cfgBaseProto.MaxTimePerBlock = cfg.MaxTimePerBlock
	cfgBaseProto.Genesis.TimePerBlock = cfg.TimePerBlock
	cfgBaseProto.SeedList = cfg.SeedNodes
	cfgBaseProto.VerifyTransactions = true
	cfgBaseProto.P2PSigExtensions = true
	cfgBaseProto.P2PNotaryRequestPayloadPoolSize = int(cfg.P2PNotaryRequestPayloadPoolSize)
	cfgBaseProto.MaxTraceableBlocks = cfg.MaxTraceableBlocks
	cfgBaseProto.Genesis.MaxTraceableBlocks = cfg.MaxTraceableBlocks
	cfgBaseProto.MaxValidUntilBlockIncrement = cfg.MaxValidUntilBlockIncrement
	cfgBaseProto.Genesis.MaxValidUntilBlockIncrement = cfg.MaxValidUntilBlockIncrement
	cfgBaseProto.Hardforks = cfg.Hardforks.Name
	if cfg.ValidatorsHistory.Height != nil {
		cfgBaseProto.ValidatorsHistory = make(map[uint32]uint32, len(cfg.ValidatorsHistory.Height))
		for height, num := range cfg.ValidatorsHistory.Height {
			cfgBaseProto.ValidatorsHistory[height] = num
		}
	} else {
		cfgBaseProto.ValidatorsCount = uint32(len(standByCommittee))
	}

	if cfg.SetRolesInGenesis {
		// note that ValidatorsHistory or ValidatorsCount field must be set above
		genesisValidatorsCount := cfgBaseProto.GetNumOfCNs(0)
		cfgBaseProto.Genesis.Roles = map[noderoles.Role]keys.PublicKeys{
			// Notary service is always enabled, see below
			noderoles.P2PNotary:     cfg.Committee[:genesisValidatorsCount],
			noderoles.NeoFSAlphabet: cfg.Committee[:genesisValidatorsCount],
		}
	}

	neowallet := neogoconfig.Wallet{
		Path:     wallet.Path,
		Password: wallet.Password,
	}

	cfgBaseApp := &cfgBase.ApplicationConfiguration
	cfgBaseApp.Relay = true
	cfgBaseApp.Consensus.Enabled = true
	cfgBaseApp.Consensus.UnlockWallet = neowallet
	cfgBaseApp.P2PNotary.Enabled = true
	cfgBaseApp.P2PNotary.UnlockWallet = neowallet
	cfgBaseApp.RPC.StartWhenSynchronized = true
	cfgBaseApp.RPC.MaxGasInvoke = fixedn.Fixed8FromInt64(int64(cfg.RPC.MaxGasInvoke))
	cfgBaseApp.RPC.MaxIteratorResultItems = 100
	cfgBaseApp.RPC.SessionEnabled = true
	cfgBaseApp.RPC.SessionExpansionEnabled = true
	cfgBaseApp.P2P.Addresses = cfg.P2P.Listen
	cfgBaseApp.P2P.DialTimeout = cfg.P2P.DialTimeout
	cfgBaseApp.P2P.ProtoTickInterval = cfg.P2P.ProtoTickInterval
	cfgBaseApp.P2P.PingInterval = cfg.P2P.Ping.Interval
	cfgBaseApp.P2P.PingTimeout = cfg.P2P.Ping.Timeout
	cfgBaseApp.P2P.MinPeers = int(cfg.P2P.Peers.Min)
	cfgBaseApp.P2P.AttemptConnPeers = int(cfg.P2P.Peers.Attempts)
	cfgBaseApp.P2P.MaxPeers = int(cfg.P2P.Peers.Max)

	cfgBaseApp.RPC.Enabled = true
	cfgBaseApp.RPC.Addresses = cfg.RPC.Listen
	cfgBaseApp.RPC.MaxWebSocketClients = int(cfg.RPC.MaxWebSocketClients)
	cfgBaseApp.RPC.SessionPoolSize = int(cfg.RPC.SessionPoolSize)
	if tlsCfg := cfg.RPC.TLS; tlsCfg.Enabled {
		cfgBaseApp.RPC.TLSConfig.Enabled = true
		cfgBaseApp.RPC.TLSConfig.Addresses = tlsCfg.Listen
		cfgBaseApp.RPC.TLSConfig.CertFile = tlsCfg.CertFile
		cfgBaseApp.RPC.TLSConfig.KeyFile = tlsCfg.KeyFile
	}

	cfgBaseApp.KeepOnlyLatestState = cfg.KeepOnlyLatestState
	cfgBaseApp.RemoveUntraceableBlocks = cfg.RemoveUntraceableBlocks

	err = cfgBase.ProtocolConfiguration.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid composed protocol configuration: %w", err)
	}

	cfgServer, err := network.NewServerConfig(cfgBase)
	if err != nil {
		return nil, fmt.Errorf("compose NeoGo server config from the base one: %w", err)
	}

	var cfgDB dbconfig.DBConfiguration
	cfgDB.Type = cfg.Storage.Type
	switch cfgDB.Type {
	case dbconfig.BoltDB:
		cfgDB.BoltDBOptions.FilePath = cfg.Storage.Path
	case dbconfig.LevelDB:
		cfgDB.LevelDBOptions.DataDirectoryPath = cfg.Storage.Path
	}

	bcStorage, err := storage.NewStore(cfgDB)
	if err != nil {
		return nil, fmt.Errorf("init storage for blockchain: %w", err)
	}

	defer func() {
		if err != nil {
			closeErr := bcStorage.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w; also failed to close blockchain storage: %w", err, closeErr)
			}
		}
	}()

	bc, err := core.NewBlockchain(bcStorage, cfgBase.Blockchain(), log, customNatives...)
	if err != nil {
		return nil, fmt.Errorf("init core blockchain component: %w", err)
	}

	netServer, err := network.NewServer(cfgServer, bc, bc.GetStateSyncModule(), log)
	if err != nil {
		return nil, fmt.Errorf("init NeoGo network server: %w", err)
	}

	var cfgNotary notary.Config
	cfgNotary.MainCfg.Enabled = true
	cfgNotary.MainCfg.UnlockWallet = neowallet
	cfgNotary.Chain = bc
	cfgNotary.Log = log

	notaryService, err := notary.NewNotary(cfgNotary, netServer.Net, netServer.GetNotaryPool(), func(tx *transaction.Transaction) error {
		err := netServer.RelayTxn(tx)
		if err != nil && !errors.Is(err, core.ErrAlreadyExists) && !errors.Is(err, core.ErrAlreadyInPool) {
			return fmt.Errorf("relay completed notary transaction %s: %w", tx.Hash().StringLE(), err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("init Notary service: %w", err)
	}

	netServer.AddService(notaryService)
	bc.SetNotary(notaryService)

	var cfgConsensus consensus.Config
	cfgConsensus.Logger = log
	cfgConsensus.Broadcast = netServer.BroadcastExtensible
	cfgConsensus.Chain = bc
	cfgConsensus.BlockQueue = netServer.GetBlockQueue()
	cfgConsensus.ProtocolConfiguration = bc.GetConfig().ProtocolConfiguration
	cfgConsensus.RequestTx = netServer.RequestTx
	cfgConsensus.StopTxFlow = netServer.StopTxFlow
	cfgConsensus.Wallet = neowallet

	consensusService, err := consensus.NewService(cfgConsensus)
	if err != nil {
		return nil, fmt.Errorf("init Consensus service: %w", err)
	}

	netServer.AddConsensusService(consensusService, consensusService.OnPayload, consensusService.OnTransaction)

	// "make" channel rw to satisfy Start method
	chErrRW := make(chan error)

	go func() {
		for {
			err, ok := <-chErrRW
			if !ok {
				return
			}
			errChan <- err
		}
	}()

	rpcServer := rpcsrv.New(bc, cfgBaseApp.RPC, netServer, nil, log, chErrRW)

	netServer.AddService(rpcServer)

	return &Blockchain{
		logger:    log,
		storage:   bcStorage,
		core:      bc,
		netServer: netServer,
		rpcServer: rpcServer,
		chErr:     chErrRW,
	}, nil
}

// Run runs the Blockchain and makes all its functionality available for use.
// Context break fails startup. Run returns any error encountered which
// prevented the Blockchain to be started. If Run failed, the Blockchain should
// no longer be used. After Blockchain has been successfully run, all internal
// failures are written to the configured listener.
//
// Run should not be called more than once.
//
// Use Stop to stop the Blockchain.
func (x *Blockchain) Run(ctx context.Context) (err error) {
	defer func() {
		// note that we can't rely on the fact that the method never returns an error
		// since this may not be forever
		if err != nil {
			closeErr := x.storage.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w; also failed to close blockchain storage: %w", err, closeErr)
			}
		}
	}()

	go x.core.Run()
	go x.netServer.Start()

	// await synchronization with the network
	t := time.NewTicker(x.core.GetConfig().TimePerBlock)

	for {
		x.logger.Info("waiting for synchronization with the blockchain network...")
		select {
		case <-ctx.Done():
			return fmt.Errorf("await state sync: %w", ctx.Err())
		case <-t.C:
			if x.netServer.IsInSync() {
				x.logger.Info("blockchain state successfully synchronized")
				return nil
			}
		}
	}
}

// Stop stops the running Blockchain and frees all its internal resources.
//
// Stop should not be called twice and before successful Run.
func (x *Blockchain) Stop() {
	x.netServer.Shutdown()
	x.core.Close()
	close(x.chErr)
}

// BuildWSClient initializes rpcclient.WSClient with direct access to the
// underlying blockchain.
func (x *Blockchain) BuildWSClient(ctx context.Context) (*rpcclient.WSClient, error) {
	internalClient, err := rpcclient.NewInternal(ctx, x.rpcServer.RegisterLocal)
	if err != nil {
		return nil, fmt.Errorf("construct internal pseudo-RPC client: %w", err)
	}

	err = internalClient.Init()
	if err != nil {
		return nil, fmt.Errorf("init internal pseudo-RPC client: %w", err)
	}

	return &internalClient.WSClient, nil
}
