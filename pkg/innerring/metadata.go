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
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	metachaingas "github.com/nspcc-dev/neofs-node/pkg/core/metachain/gas"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain"
	"go.uber.org/zap"
)

func enableMetadataChain(ctx context.Context, server *Server, cfg *config.Config, errChan chan<- error) (*notary.Actor, error) {
	v, err := server.fsChainClient.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("fetching FS chain version: %w", err)
	}

	metaSeeds, err := changePort(cfg.FSChain.Consensus.SeedNodes, cfg.Experimental.ChainMetaData.SeedPort)
	if err != nil {
		return nil, fmt.Errorf("parsing consensus seed nodes: %w", err)
	}
	metaRPCs, err := changePort(cfg.FSChain.Consensus.RPC.Listen, cfg.Experimental.ChainMetaData.RPCPort)
	if err != nil {
		return nil, fmt.Errorf("parsing consensus RPCs: %w", err)
	}
	metaP2Ps, err := changePort(cfg.FSChain.Consensus.P2P.Listen, cfg.Experimental.ChainMetaData.P2PPort)
	if err != nil {
		return nil, fmt.Errorf("parsing consensus P2Ps: %w", err)
	}

	fsChainProtocol := v.Protocol
	metaChainCfg := config.Consensus{
		Magic:     uint32(fsChainProtocol.Network) + 1,
		Committee: fsChainProtocol.StandbyCommittee,
		Storage: config.Storage{
			Path: path.Join(path.Dir(cfg.FSChain.Consensus.Storage.Path), "meta_db.bolt"),
			Type: dbconfig.BoltDB,
		},
		TimePerBlock:                50 * time.Millisecond,
		MaxTimePerBlock:             20 * time.Second,
		MaxTraceableBlocks:          fsChainProtocol.MaxTraceableBlocks,
		MaxValidUntilBlockIncrement: fsChainProtocol.MaxValidUntilBlockIncrement,
		SeedNodes:                   metaSeeds,
		Hardforks:                   config.Hardforks{},
		ValidatorsHistory:           config.ValidatorsHistory{},
		RPC: config.RPC{
			Listen:              metaRPCs,
			MaxWebSocketClients: cfg.FSChain.Consensus.RPC.MaxWebSocketClients,
			SessionPoolSize:     cfg.FSChain.Consensus.RPC.SessionPoolSize,
			MaxGasInvoke:        cfg.FSChain.Consensus.RPC.MaxGasInvoke,
		},
		P2P: config.P2P{
			DialTimeout:       cfg.FSChain.Consensus.P2P.DialTimeout,
			ProtoTickInterval: cfg.FSChain.Consensus.P2P.ProtoTickInterval,
			Listen:            metaP2Ps,
			Peers:             cfg.FSChain.Consensus.P2P.Peers,
			Ping:              cfg.FSChain.Consensus.P2P.Ping,
		},
		SetRolesInGenesis:               true,
		KeepOnlyLatestState:             false,
		RemoveUntraceableBlocks:         false,
		P2PNotaryRequestPayloadPoolSize: 1000, // default for blockchain.New()
	}

	server.metaChain, err = metachain.NewMetaChain(&metaChainCfg, &cfg.Wallet, errChan, server.log.With(zap.String("component", "metadata chain (IR)")))
	if err != nil {
		return nil, fmt.Errorf("init meta sidechain blockchain: %w", err)
	}
	server.workers = append(server.workers, func(ctx context.Context) error {
		return server.metaChain.Run(ctx)
	})
	server.registerCloser(func() error {
		server.metaChain.Stop()
		return nil
	})

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
	metaActor, err := notary.NewActor(metaCli, []actor.SignerAccount{
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
		if err != nil {
			return fmt.Errorf("new meta notary actor: %w", err)
		}

		gasAct := gas.New(act)
		txHash, vub, err := gasAct.Transfer(
			simpleAccHash,
			notary.Hash,
			big.NewInt(metachaingas.DefaultBalance*9/10), // default metadata chain balance but a little bit less
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

	return metaActor, nil
}

func changePort(addrs []string, port uint16) ([]string, error) {
	res := slices.Clone(addrs)
	for i := range res {
		host, _, err := net.SplitHostPort(res[i])
		if err != nil {
			return nil, fmt.Errorf("[%d] address ('%s') cannot be parsed: %w", i, res[i], err)
		}
		res[i] = net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10))
	}

	return res, nil
}
