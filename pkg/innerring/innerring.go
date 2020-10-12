package innerring

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/balance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/container"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	"github.com/pkg/errors"
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
		localTimers     *timers.Timers

		// global state
		morphClient    *client.Client
		mainnetClient  *client.Client
		epochCounter   atomic.Uint64
		innerRingIndex atomic.Int32

		// todo: export error channel
	}

	contracts struct {
		neofs      util.Uint160 // in mainnet
		netmap     util.Uint160 // in morph
		balance    util.Uint160 // in morph
		container  util.Uint160 // in morph
		audit      util.Uint160 // in morph
		reputation util.Uint160 // in morph
		neofsid    util.Uint160 // in morph
		gas        util.Uint160 // native contract in both chains

		alphabet [alphabetContractsN]util.Uint160 // in morph
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

	alphabetContractsN = 7 // az, buky, vedi, glagoli, dobro, jest, zhivete
)

// Start runs all event providers.
func (s *Server) Start(ctx context.Context) error {
	s.localTimers.Start(ctx) // local timers start ticking

	go s.morphListener.Listen(ctx)   // listen for neo:morph events
	go s.mainnetListener.Listen(ctx) // listen for neo:mainnet events

	return nil
}

// Stop closes all subscription channels.
func (s *Server) Stop() {
	go s.morphListener.Stop()
	go s.mainnetListener.Stop()
}

// New creates instance of inner ring sever structure.
func New(ctx context.Context, log *zap.Logger, cfg *viper.Viper) (*Server, error) {
	server := &Server{log: log}

	// prepare inner ring node private key
	key, err := crypto.LoadPrivateKey(cfg.GetString("key"))
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't create private key")
	}

	// get all script hashes of contracts
	contracts, err := parseContracts(cfg)
	if err != nil {
		return nil, err
	}

	// create local timer instance
	server.localTimers = timers.New(&timers.Params{
		Log:              log,
		EpochDuration:    cfg.GetDuration("timers.epoch"),
		AlphabetDuration: cfg.GetDuration("timers.emit"),
	})

	morphChain := &chainParams{
		log:  log,
		cfg:  cfg,
		key:  key,
		gas:  contracts.gas,
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

	// create netmap processor
	netmapProcessor, err := netmap.New(&netmap.Params{
		Log:            log,
		PoolSize:       cfg.GetInt("workers.netmap"),
		NetmapContract: contracts.netmap,
		EpochTimer:     server.localTimers,
		MorphClient:    server.morphClient,
		EpochState:     server,
		ActiveState:    server,
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
		ContainerContract: contracts.container,
		MorphClient:       server.morphClient,
		ActiveState:       server,
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
		NeoFSContract:   contracts.neofs,
		BalanceContract: contracts.balance,
		MainnetClient:   server.mainnetClient,
		ActiveState:     server,
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
		Log:             log,
		PoolSize:        cfg.GetInt("workers.neofs"),
		NeoFSContract:   contracts.neofs,
		BalanceContract: contracts.balance,
		NetmapContract:  contracts.netmap,
		MorphClient:     server.morphClient,
		EpochState:      server,
		ActiveState:     server,
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
		AlphabetContracts: contracts.alphabet,
		MorphClient:       server.morphClient,
		IRList:            server,
	})
	if err != nil {
		return nil, err
	}

	err = bindMorphProcessor(alphabetProcessor, server)
	if err != nil {
		return nil, err
	}

	// todo: create vivid id component
	// todo: create audit scheduler

	err = initConfigFromBlockchain(server, contracts, &key.PublicKey)
	if err != nil {
		return nil, errors.Wrap(err, "initializing error")
	}

	return server, nil
}

func createListener(ctx context.Context, p *chainParams) (event.Listener, error) {
	sub, err := subscriber.New(ctx, &subscriber.Params{
		Log:         p.log,
		Endpoint:    p.cfg.GetString(p.name + ".endpoint.notification"),
		DialTimeout: p.cfg.GetDuration(p.name + ".dial_timeouts"),
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
		client.WithDialTimeout(p.cfg.GetDuration(p.name+".dial_timeouts")),
		client.WithMagic(netmode.Magic(p.cfg.GetUint32(p.name+".magic_number"))),
		client.WithGasContract(p.gas),
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
	nativeGasContractStr := cfg.GetString("contracts.gas")
	containerContractStr := cfg.GetString("contracts.container")

	result.netmap, err = util.Uint160DecodeStringLE(netmapContractStr)
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't read netmap script-hash")
	}

	result.neofs, err = util.Uint160DecodeStringLE(neofsContractStr)
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't read neofs script-hash")
	}

	result.balance, err = util.Uint160DecodeStringLE(balanceContractStr)
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't read balance script-hash")
	}

	result.gas, err = util.Uint160DecodeStringLE(nativeGasContractStr)
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't read native gas script-hash")
	}

	result.container, err = util.Uint160DecodeStringLE(containerContractStr)
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't read container script-hash")
	}

	result.alphabet, err = parseAlphabetContracts(cfg)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func parseAlphabetContracts(cfg *viper.Viper) (res [7]util.Uint160, err error) {
	// list of glagolic script letters that represent alphabet contracts
	glagolic := []string{"az", "buky", "vedi", "glagoli", "dobro", "jest", "zhivete"}

	for i, letter := range glagolic {
		contractStr := cfg.GetString("contracts.alphabet." + letter)

		res[i], err = util.Uint160DecodeStringLE(contractStr)
		if err != nil {
			return res, errors.Wrapf(err, "ir: can't read alphabet %s contract", letter)
		}
	}

	return res, nil
}

func initConfigFromBlockchain(s *Server, c *contracts, key *ecdsa.PublicKey) error {
	// get current epoch
	epoch, err := invoke.Epoch(s.morphClient, c.netmap)
	if err != nil {
		return errors.Wrap(err, "can't read epoch")
	}

	// check if node inside inner ring list and what index it has
	index, err := invoke.InnerRingIndex(s.mainnetClient, c.neofs, key)
	if err != nil {
		return errors.Wrap(err, "can't read inner ring list")
	}

	s.epochCounter.Store(uint64(epoch))
	s.innerRingIndex.Store(index)

	s.log.Debug("read config from blockchain",
		zap.Bool("active", s.IsActive()),
		zap.Int64("epoch", epoch),
	)

	return nil
}
