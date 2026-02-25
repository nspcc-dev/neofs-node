package sidechain

import (
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/network"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv"
	"github.com/nspcc-dev/neofs-node/pkg/core/metachain"
	"go.uber.org/zap"
)

// TODO
type SideChain struct {
	logger    *zap.Logger
	storage   storage.Store
	core      *core.Blockchain
	netServer *network.Server
	rpcServer *rpcsrv.Server

	magicNumber uint32

	chErr chan error
}

// TODO
func New(cfg config.Config, log *zap.Logger, errCh chan error) (*SideChain, error) {
	store, err := storage.NewStore(cfg.ApplicationConfiguration.DBConfiguration)
	if err != nil {
		return &SideChain{}, fmt.Errorf("could not initialize storage: %w", err)
	}
	defer func() {
		if err != nil {
			closeErr := store.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w; also failed to close blockchain storage: %w", err, closeErr)
			}
		}
	}()

	chain, err := core.NewBlockchain(store, cfg.Blockchain(), log.With(zap.String("subcomponent", "core chain")), metachain.NewCustomNatives)
	if err != nil {
		return &SideChain{}, fmt.Errorf("initializing meta block chain: %w", err)
	}

	cfgServer, err := network.NewServerConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("compose NeoGo server config from the base one: %w", err)
	}

	netServer, err := network.NewServer(cfgServer, chain, chain.GetStateSyncModule(), log.With(zap.String("subcomponent", "network server")))
	if err != nil {
		return nil, fmt.Errorf("init NeoGo network server: %w", err)
	}

	chErr := make(chan error)
	go func() {
		for {
			err, ok := <-chErr
			if !ok {
				return
			}
			errCh <- err
		}
	}()

	rpcServer := rpcsrv.New(chain, cfg.ApplicationConfiguration.RPC, netServer, nil, log.With(zap.String("subcomponent", "rpc server")), chErr)
	netServer.AddService(rpcServer)

	return &SideChain{
		logger:      log,
		storage:     store,
		core:        chain,
		netServer:   netServer,
		rpcServer:   rpcServer,
		chErr:       chErr,
		magicNumber: uint32(cfg.ProtocolConfiguration.Magic),
	}, nil
}

// TODO
func (s *SideChain) Run(ctx context.Context) error {
	var err error
	defer func() {
		// note that we can't rely on the fact that the method never returns an error
		// since this may not be forever
		if err != nil {
			closeErr := s.storage.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w; also failed to close blockchain storage: %w", err, closeErr)
			}
		}
	}()

	go s.core.Run()
	go s.netServer.Start()

	t := time.NewTicker(s.core.GetConfig().ProtocolConfiguration.Genesis.TimePerBlock)

	for {
		s.logger.Info("waiting for synchronization with the blockchain network...")
		select {
		case <-ctx.Done():
			return fmt.Errorf("await state sync: %w", context.Cause(ctx))
		case <-t.C:
			if s.netServer.IsInSync() {
				s.logger.Info("blockchain state successfully synchronized")
				return nil
			}
		}
	}
}

// TODO
func (s *SideChain) Stop() {
	s.netServer.Shutdown()
	s.core.Close()
	close(s.chErr)
}

// TODO
func (s *SideChain) Height() uint32 {
	return s.core.HeaderHeight()
}

// TODO
func (s *SideChain) AddTx(tx *transaction.Transaction) error {
	return s.netServer.RelayTxn(tx)
}

// TODO
func (s *SideChain) Magic() uint32 {
	return s.magicNumber
}

// TODO
func (s *SideChain) SubscribeForNotifications(ch chan *state.ContainedNotificationEvent) {
	s.core.SubscribeForNotifications(ch)
}

// TODO
func (s *SideChain) SubscribeForBlocks(ch chan *block.Header) {
	s.core.SubscribeForHeadersOfAddedBlocks(ch)
}
