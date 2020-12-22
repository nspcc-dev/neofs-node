package innerring

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/balance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/container"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/subscriber"
	auditSvc "github.com/nspcc-dev/neofs-node/pkg/services/audit"
	audittask "github.com/nspcc-dev/neofs-node/pkg/services/audit/taskmanager"
	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
	"github.com/panjf2000/ants/v2"
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
		innerRingSize  atomic.Int32
		precision      precision.Fixed8Converter
		auditClient    *auditWrapper.ClientWrapper

		// internal variables
		key                  *ecdsa.PrivateKey
		pubKey               []byte
		contracts            *contracts
		predefinedValidators []keys.PublicKey

		workers []func(context.Context)
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
func (s *Server) Start(ctx context.Context, intError chan<- error) error {
	err := s.initConfigFromBlockchain()
	if err != nil {
		return err
	}

	// vote for sidechain validator if it is prepared in config
	err = s.voteForSidechainValidator(s.predefinedValidators)
	if err != nil {
		// we don't stop inner ring execution on this error
		s.log.Warn("can't vote for prepared validators",
			zap.String("error", err.Error()))
	}

	s.localTimers.Start(ctx) // local timers start ticking

	morphErr := make(chan error)
	mainnnetErr := make(chan error)

	// anonymous function to multiplex error channels
	go func() {
		select {
		case <-ctx.Done():
			return
		case err := <-morphErr:
			intError <- errors.Wrap(err, "sidechain")
		case err := <-mainnnetErr:
			intError <- errors.Wrap(err, "mainnet")
		}
	}()

	go s.morphListener.ListenWithError(ctx, morphErr)      // listen for neo:morph events
	go s.mainnetListener.ListenWithError(ctx, mainnnetErr) // listen for neo:mainnet events

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
}

func (s *Server) WriteReport(r *auditSvc.Report) error {
	res := r.Result()
	res.SetPublicKey(s.pubKey)

	return s.auditClient.PutAuditResult(res)
}

// New creates instance of inner ring sever structure.
func New(ctx context.Context, log *zap.Logger, cfg *viper.Viper) (*Server, error) {
	var err error
	server := &Server{log: log}

	// prepare inner ring node private key
	server.key, err = crypto.LoadPrivateKey(cfg.GetString("key"))
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't create private key")
	}

	// get all script hashes of contracts
	server.contracts, err = parseContracts(cfg)
	if err != nil {
		return nil, err
	}

	// parse default validators
	server.predefinedValidators, err = parsePredefinedValidators(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't parse predefined validators list")
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
		key:  server.key,
		gas:  server.contracts.gas,
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

	server.pubKey = crypto.MarshalPublicKey(&server.key.PublicKey)

	auditPool, err := ants.NewPool(cfg.GetInt("audit.task.exec_pool_size"), ants.WithNonblocking(true))
	if err != nil {
		return nil, err
	}

	server.auditClient, err = invoke.NewNoFeeAuditClient(server.morphClient, server.contracts.audit)
	if err != nil {
		return nil, err
	}

	auditTaskManager := audittask.New(
		audittask.WithQueueCapacity(cfg.GetUint32("audit.task.queue_capacity")),
		audittask.WithWorkerPool(auditPool),
		audittask.WithLogger(log),
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
		ClientCache:       newClientCache(server.key),
		TaskManager:       auditTaskManager,
		Reporter:          server,
	})
	if err != nil {
		return nil, err
	}

	// create netmap processor
	netmapProcessor, err := netmap.New(&netmap.Params{
		Log:              log,
		PoolSize:         cfg.GetInt("workers.netmap"),
		NetmapContract:   server.contracts.netmap,
		EpochTimer:       server.localTimers,
		MorphClient:      server.morphClient,
		EpochState:       server,
		ActiveState:      server,
		CleanupEnabled:   cfg.GetBool("netmap_cleaner.enabled"),
		CleanupThreshold: cfg.GetUint64("netmap_cleaner.threshold"),
		HandleAudit:      auditProcessor.StartAuditHandler(),
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
		NeoFSContract:   server.contracts.neofs,
		BalanceContract: server.contracts.balance,
		MainnetClient:   server.mainnetClient,
		ActiveState:     server,
		Converter:       &server.precision,
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
		Log:               log,
		PoolSize:          cfg.GetInt("workers.neofs"),
		NeoFSContract:     server.contracts.neofs,
		BalanceContract:   server.contracts.balance,
		NetmapContract:    server.contracts.netmap,
		MorphClient:       server.morphClient,
		EpochState:        server,
		ActiveState:       server,
		Converter:         &server.precision,
		MintEmitCacheSize: cfg.GetInt("emit.mint.cache_size"),
		MintEmitThreshold: cfg.GetUint64("emit.mint.threshold"),
		MintEmitValue:     fixedn.Fixed8(cfg.GetInt64("emit.mint.value")),
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

	// todo: create vivid id component

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
	auditContractStr := cfg.GetString("contracts.audit")

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

	result.audit, err = util.Uint160DecodeStringLE(auditContractStr)
	if err != nil {
		return nil, errors.Wrap(err, "ir: can't read audit script-hash")
	}

	result.alphabet, err = parseAlphabetContracts(cfg)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func parsePredefinedValidators(cfg *viper.Viper) ([]keys.PublicKey, error) {
	publicKeyStrings := cfg.GetStringSlice("morph.validators")

	return ParsePublicKeysFromStrings(publicKeyStrings)
}

// ParsePublicKeysFromStrings returns slice of neo public keys from slice
// of hex encoded strings.
func ParsePublicKeysFromStrings(pubKeys []string) ([]keys.PublicKey, error) {
	publicKeys := make([]keys.PublicKey, 0, len(pubKeys))

	for i := range pubKeys {
		key, err := keys.NewPublicKeyFromString(pubKeys[i])
		if err != nil {
			return nil, errors.Wrap(err, "can't decode public key")
		}

		publicKeys = append(publicKeys, *key)
	}

	return publicKeys, nil
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

func (s *Server) initConfigFromBlockchain() error {
	// get current epoch
	epoch, err := invoke.Epoch(s.morphClient, s.contracts.netmap)
	if err != nil {
		return errors.Wrap(err, "can't read epoch")
	}

	key := &s.key.PublicKey

	// check if node inside inner ring list and what index it has
	index, size, err := invoke.InnerRingIndex(s.mainnetClient, s.contracts.neofs, key)
	if err != nil {
		return errors.Wrap(err, "can't read inner ring list")
	}

	// get balance precision
	balancePrecision, err := invoke.BalancePrecision(s.morphClient, s.contracts.balance)
	if err != nil {
		return errors.Wrap(err, "can't read balance contract precision")
	}

	s.epochCounter.Store(uint64(epoch))
	s.innerRingSize.Store(size)
	s.innerRingIndex.Store(index)
	s.precision.SetBalancePrecision(balancePrecision)

	s.log.Debug("read config from blockchain",
		zap.Bool("active", s.IsActive()),
		zap.Int64("epoch", epoch),
		zap.Uint32("precision", balancePrecision),
	)

	return nil
}
