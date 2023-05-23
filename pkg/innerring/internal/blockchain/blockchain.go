package blockchain

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/consensus"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/network"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/services/notary"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	utilConfig "github.com/nspcc-dev/neofs-node/pkg/util/config"
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

	nodeAcc *wallet.Account

	chErr chan error
}

// StorageConfig configures Blockchain storage.
type StorageConfig struct {
	typ  string
	path string
}

// BoltDB configures Blockchain to use BoltDB located in given path.
func BoltDB(path string) StorageConfig {
	return StorageConfig{
		typ:  dbconfig.BoltDB,
		path: path,
	}
}

// LevelDB configures Blockchain to use LevelDB located in given path.
func LevelDB(path string) StorageConfig {
	return StorageConfig{
		typ:  dbconfig.LevelDB,
		path: path,
	}
}

// InMemory configures Blockchain to use volatile RAM storage.
func InMemory() StorageConfig {
	return StorageConfig{
		typ: dbconfig.InMemoryDB,
	}
}

// PingConfig configures P2P pinging mechanism.
type PingConfig struct {
	// Interval between pings.
	//
	// Optional: defaults to 30s. Must not be negative.
	Interval time.Duration

	// Time period to wait for pong.
	//
	// Optional: defaults to 1m. Must not be negative.
	Timeout time.Duration
}

// RPCConfig configures RPC serving.
type RPCConfig struct {
	// Network addresses to listen Neo RPC on. Each element must be a valid TCP
	// address in 'host:port' format.
	//
	// Optional: by default, insecure Neo RPC is not served.
	Addresses []string

	// Additional addresses that use TLS.
	//
	// Optional.
	TLSConfig
}

// TLSConfig configures additional RPC serving over TLS.
type TLSConfig struct {
	// Additional TLS serving switcher.
	//
	// Optional: by default TLS is switched off.
	Enabled bool

	// Network addresses to listen Neo RPC on if Enabled. Each element must be a valid TCP
	// address in 'host:port' format.
	Addresses []string

	// TLS certificate file path.
	//
	// Required if Enabled and one or more addresses are provided.
	CertFile string

	// TLS private key file path.
	//
	// Required if Enabled and one or more addresses are provided.
	KeyFile string
}

// P2PConfig configures communication over Neo P2P protocol.
type P2PConfig struct {
	// Specifies the minimum number of peers a node needs for normal operation.
	//
	// Required. Must not be larger than math.MaxInt32.
	MinPeers uint

	// Specifies how many peers node should try to dial when connection counter
	// drops below the MinPeers value.
	//
	// Optional: defaults to MinPeers+10. Must not be greater than math.MaxInt32.
	AttemptConnPeers uint

	// Limits maximum number of peers dealing with the node.
	//
	// Optional: defaults to 100. Must not be larger than math.MaxInt32.
	MaxPeers uint

	// Pinging mechanism.
	//
	// Optional: see P2PConfig defaults.
	Ping PingConfig

	// Maximum duration a single dial may take.
	//
	// Optional: defaults to 5s. Must not be negative.
	DialTimeout time.Duration

	// Interval between protocol ticks with each connected peer.
	//
	// Optional: defaults to 2s. Must not be negative.
	ProtoTickInterval time.Duration

	// Network addresses to listen Neo P2P on. Each element must be a valid TCP
	// address in 'host:port' format.
	//
	// Optional: by default, Neo P2P is not served.
	ListenAddresses []string
}

// Config configures Blockchain. All required fields must be set. Specified
// optional fields tune Blockchain's default behavior (zero or omitted values).
//
// See docs of NeoGo configuration for some details.
type Config struct {
	// Writer of the Blockchain's logs and internal errors.
	//
	// Optional: by default, Blockchain doesn't write logs.
	Logger *zap.Logger

	// Application level error listener. Blockchain writes any internal error to the
	// channel. Error returns of functions (e.g. New) are not pushed. The channel
	// should be regularly checked in order to prevent blocking. Any pop-up error
	// does not allow the Blockchain to fully work, therefore, if it is detected,
	// the blockchain should be stopped.
	//
	// Required.
	ErrorListener chan<- error

	// Identifier of the Neo network.
	//
	// Required.
	NetworkMagic netmode.Magic

	// Initial committee staff.
	//
	// Required.
	Committee keys.PublicKeys

	// Time period (approximate) between two adjacent blocks.
	//
	// Optional: defaults to 15s. Must not be negative.
	BlockInterval time.Duration

	// Neo RPC service configuration.
	//
	// Optional: see RPCConfig defaults.
	RPC RPCConfig

	// Length of the chain accessible to smart contracts.
	//
	// Optional: defaults to 2102400.
	TraceableChainLength uint32

	// Maps hard-fork's name to the appearance chain height.
	//
	// Optional: by default, each known hard-fork is applied from the zero
	// blockchain height.
	HardForks map[string]uint32

	// List of nodes' addresses to communicate with over Neo P2P protocol in
	// 'host:port' format.
	//
	// Optional: by default, node runs as standalone.
	SeedNodes []string

	// P2P settings.
	//
	// Required.
	P2P P2PConfig

	// Storage configuration. Must be set using one of constructors like BoltDB.
	//
	// Required.
	Storage StorageConfig

	// NEO wallet of the node. The wallet is used by Consensus and Notary services.
	// Corresponding private key be accessed via Blockchain.LocalKey.
	//
	// Required.
	Wallet config.Wallet

	// Maps chain height to number of consensus nodes.
	//
	// Optional: by default Committee size is used. Each value must not be greater
	// than math.MaxInt32.
	ValidatorsHistory map[uint32]uint32

	// Chronology of native contracts updates. Maps name of Neo native contract to
	// chain heights.
	//
	// Optional: by default, all native contracts are active from genesis block.
	// Keys must be valid native names. Values must not be empty.
	NativeActivations map[string][]uint32
}

// New returns new Blockchain configured by the specified Config. New panics if
// any required Config field is zero or unset. Resulting Blockchain is ready to
// run. Launched Blockchain should be finally stopped.
func New(cfg Config) (res *Blockchain, err error) {
	switch {
	case cfg.Storage.typ == "":
		panic("uninitialized storage config")
	case cfg.BlockInterval < 0:
		panic("negative block interval")
	case cfg.Wallet.Path == "":
		panic("missing wallet path")
	case cfg.ErrorListener == nil:
		panic("missing error channel")
	case cfg.NetworkMagic == 0:
		// actually zero magic is valid, but almost definitely forgotten
		panic("missing network magic")
	case len(cfg.Committee) == 0:
		panic("empty committee")
	case cfg.P2P.MinPeers > math.MaxInt32:
		panic(fmt.Sprintf("min peers is out of allowable range %d", cfg.P2P.MinPeers))
	case cfg.P2P.MaxPeers > math.MaxInt32:
		panic(fmt.Sprintf("max peers is out of allowable range %d", cfg.P2P.MaxPeers))
	case cfg.P2P.AttemptConnPeers > math.MaxInt32:
		panic(fmt.Sprintf("connection attempts number is out of allowable range %d", cfg.P2P.AttemptConnPeers))
	case cfg.P2P.Ping.Interval < 0:
		panic("negative ping interval")
	case cfg.P2P.Ping.Timeout < 0:
		panic("negative ping timeout")
	case cfg.P2P.DialTimeout < 0:
		panic("negative dial timeout")
	case cfg.P2P.ProtoTickInterval < 0:
		panic("negative proto tick interval")
	}

	for height, num := range cfg.ValidatorsHistory {
		if num > math.MaxInt32 {
			panic(fmt.Sprintf("number of validators at height %d is out of allowable range %d", height, num))
		}
	}

	for name, heights := range cfg.NativeActivations {
		if !nativenames.IsValid(name) {
			panic(fmt.Sprintf("invalid native name %s", name))
		} else if len(heights) == 0 {
			panic(fmt.Sprintf("empty height list %s", name))
		}
	}

	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.BlockInterval == 0 {
		cfg.BlockInterval = 15 * time.Second
	}
	if cfg.P2P.MaxPeers == 0 {
		cfg.P2P.MaxPeers = 100
	}
	if cfg.P2P.AttemptConnPeers == 0 {
		cfg.P2P.AttemptConnPeers = cfg.P2P.MinPeers + 10
	}
	if cfg.P2P.DialTimeout == 0 {
		cfg.P2P.DialTimeout = 5 * time.Second
	}
	if cfg.P2P.ProtoTickInterval == 0 {
		cfg.P2P.ProtoTickInterval = 2 * time.Second
	}
	if cfg.TraceableChainLength == 0 {
		cfg.TraceableChainLength = 2102400
	}
	if cfg.P2P.Ping.Interval == 0 {
		cfg.P2P.Ping.Interval = 30 * time.Second
	}
	if cfg.P2P.Ping.Timeout == 0 {
		cfg.P2P.Ping.Timeout = time.Minute
	}

	nodeAcc, err := utilConfig.LoadAccount(cfg.Wallet.Path, "", cfg.Wallet.Password)
	if err != nil {
		return nil, fmt.Errorf("read node account: %w", err)
	}

	standByCommittee := make([]string, len(cfg.Committee))
	for i := range cfg.Committee {
		standByCommittee[i] = hex.EncodeToString(cfg.Committee[i].Bytes())
	}

	var cfgBase config.Config

	cfgBaseProto := &cfgBase.ProtocolConfiguration
	cfgBaseProto.Magic = cfg.NetworkMagic
	cfgBaseProto.StandbyCommittee = standByCommittee

	cfgBaseProto.TimePerBlock = cfg.BlockInterval
	cfgBaseProto.SeedList = cfg.SeedNodes
	cfgBaseProto.VerifyTransactions = true
	cfgBaseProto.P2PSigExtensions = true
	cfgBaseProto.MaxTraceableBlocks = cfg.TraceableChainLength
	cfgBaseProto.Hardforks = cfg.HardForks
	if cfg.ValidatorsHistory != nil {
		cfgBaseProto.ValidatorsHistory = make(map[uint32]int, len(cfg.ValidatorsHistory))
		for height, num := range cfg.ValidatorsHistory {
			cfgBaseProto.ValidatorsHistory[height] = int(num)
		}
	} else {
		cfgBaseProto.ValidatorsCount = len(standByCommittee)
	}
	cfgBaseProto.NativeUpdateHistories = cfg.NativeActivations

	cfgBaseApp := &cfgBase.ApplicationConfiguration
	cfgBaseApp.Relay = true
	cfgBaseApp.UnlockWallet = cfg.Wallet
	cfgBaseApp.Consensus.Enabled = true
	cfgBaseApp.Consensus.UnlockWallet = cfg.Wallet
	cfgBaseApp.P2PNotary.Enabled = true
	cfgBaseApp.P2PNotary.UnlockWallet = cfg.Wallet
	cfgBaseApp.RPC.StartWhenSynchronized = true
	cfgBaseApp.RPC.MaxGasInvoke = fixedn.Fixed8FromInt64(100)
	cfgBaseApp.P2P.Addresses = cfg.P2P.ListenAddresses
	cfgBaseApp.P2P.DialTimeout = cfg.P2P.DialTimeout
	cfgBaseApp.P2P.ProtoTickInterval = cfg.P2P.ProtoTickInterval
	cfgBaseApp.P2P.PingInterval = cfg.P2P.Ping.Interval
	cfgBaseApp.P2P.PingTimeout = cfg.P2P.Ping.Timeout
	cfgBaseApp.P2P.MinPeers = int(cfg.P2P.MinPeers)
	cfgBaseApp.P2P.AttemptConnPeers = int(cfg.P2P.AttemptConnPeers)
	cfgBaseApp.P2P.MaxPeers = int(cfg.P2P.MaxPeers)

	cfgBaseApp.RPC.Enabled = true
	cfgBaseApp.RPC.Addresses = cfg.RPC.Addresses
	if tlsCfg := cfg.RPC.TLSConfig; tlsCfg.Enabled {
		cfgBaseApp.RPC.TLSConfig.Enabled = true
		cfgBaseApp.RPC.TLSConfig.Addresses = tlsCfg.Addresses
		cfgBaseApp.RPC.TLSConfig.CertFile = tlsCfg.CertFile
		cfgBaseApp.RPC.TLSConfig.KeyFile = tlsCfg.KeyFile
	}

	err = cfgBase.ProtocolConfiguration.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid composed protocol configuration: %w", err)
	}

	cfgServer, err := network.NewServerConfig(cfgBase)
	if err != nil {
		return nil, fmt.Errorf("compose NeoGo server config from the base one: %w", err)
	}

	var cfgDB dbconfig.DBConfiguration
	cfgDB.Type = cfg.Storage.typ
	if cfgDB.Type == dbconfig.BoltDB {
		cfgDB.BoltDBOptions.FilePath = cfg.Storage.path
	} else if cfgDB.Type == dbconfig.LevelDB {
		cfgDB.LevelDBOptions.DataDirectoryPath = cfg.Storage.path
	}

	bcStorage, err := storage.NewStore(cfgDB)
	if err != nil {
		return nil, fmt.Errorf("init storage for blockchain: %w", err)
	}

	defer func() {
		if err != nil {
			closeErr := bcStorage.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w; also failed to close blockchain storage: %v", err, closeErr)
			}
		}
	}()

	bc, err := core.NewBlockchain(bcStorage, cfgBase.Blockchain(), cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("init core blockchain component: %w", err)
	}

	netServer, err := network.NewServer(cfgServer, bc, bc.GetStateSyncModule(), cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("init NeoGo network server: %w", err)
	}

	var cfgNotary notary.Config
	cfgNotary.MainCfg.Enabled = true
	cfgNotary.MainCfg.UnlockWallet = cfg.Wallet
	cfgNotary.Chain = bc
	cfgNotary.Log = cfg.Logger

	notaryService, err := notary.NewNotary(cfgNotary, netServer.Net, netServer.GetNotaryPool(), func(tx *transaction.Transaction) error {
		err := netServer.RelayTxn(tx)
		if err != nil && !errors.Is(err, core.ErrAlreadyExists) {
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
	cfgConsensus.Logger = cfg.Logger
	cfgConsensus.Broadcast = netServer.BroadcastExtensible
	cfgConsensus.Chain = bc
	cfgConsensus.BlockQueue = netServer.GetBlockQueue()
	cfgConsensus.ProtocolConfiguration = bc.GetConfig().ProtocolConfiguration
	cfgConsensus.RequestTx = netServer.RequestTx
	cfgConsensus.StopTxFlow = netServer.StopTxFlow
	cfgConsensus.TimePerBlock = cfg.BlockInterval
	cfgConsensus.Wallet = cfg.Wallet

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
			cfg.ErrorListener <- err
		}
	}()

	rpcServer := rpcsrv.New(bc, cfgBaseApp.RPC, netServer, nil, cfg.Logger, chErrRW)

	netServer.AddService(&rpcServer)

	return &Blockchain{
		nodeAcc:   nodeAcc,
		logger:    cfg.Logger,
		storage:   bcStorage,
		core:      bc,
		netServer: netServer,
		rpcServer: &rpcServer,
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
				err = fmt.Errorf("%w; also failed to close blockchain storage: %v", err, closeErr)
			}
		}
	}()

	go x.core.Run()
	go x.netServer.Start(x.chErr)

	// await synchronization with the network
	t := time.NewTicker(x.core.GetConfig().TimePerBlock)
	defer t.Stop()

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

// LocalKey returns keys.PrivateKey corresponding to the configured wallet.
func (x *Blockchain) LocalKey() *keys.PrivateKey {
	return x.nodeAcc.PrivateKey()
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
