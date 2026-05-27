package innerring

import (
	"bytes"
	"context"
	"crypto/elliptic"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"path"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	metachaingas "github.com/nspcc-dev/neofs-node/pkg/core/metachain/gas"
	"github.com/nspcc-dev/neofs-node/pkg/core/metachain/meta"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

type metadataNotaryClient struct {
	indexer *innerRingIndexer
	l       *zap.Logger
	metaCli *rpcclient.WSClient
	nodeKey *keys.PrivateKey
}

func (nc *metadataNotaryClient) buildNotaryActor() *notary.Actor {
	keysIndex, err := nc.indexer.update()
	if err != nil {
		nc.l.Warn("check alphabet list", zap.Error(err))
		return nil
	}
	if keysIndex.alphabetIndex == -1 {
		nc.l.Debug("node is not part of Alphabet list, do not register metadata container")
		return nil
	}

	alphaAcc := wallet.NewAccountFromPrivateKey(nc.nodeKey)
	err = alphaAcc.ConvertMultisig(sc.GetMajorityHonestNodeCount(len(keysIndex.alphabetList)), keysIndex.alphabetList)
	if err != nil {
		nc.l.Warn("converting meta committee multi account", zap.Error(err))
		return nil
	}

	metaActor, err := notary.NewActor(nc.metaCli, []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: alphaAcc.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			},
			Account: alphaAcc,
		},
	}, wallet.NewAccountFromPrivateKey(nc.nodeKey))
	if err != nil {
		nc.l.Warn("build meta committee actor", zap.Error(err))
		return nil
	}

	return metaActor
}

func (nc *metadataNotaryClient) RegisterMetadataContainer(cID cid.ID, nonce uint32) error {
	act := nc.buildNotaryActor()
	if act == nil {
		return nil
	}

	return callMetaContractAndWait(act, nonce, "registerMetaContainer", cID[:])
}

func (nc *metadataNotaryClient) UpdateContainerPlacement(cID cid.ID, vectors [][]netmap.NodeInfo, policy netmap.PlacementPolicy, nonce uint32) error {
	act := nc.buildNotaryActor()
	if act == nil {
		return nil
	}

	var (
		placement meta.Placement
		replicas  = policy.Replicas()
	)
	for i, v := range vectors {
		var cnrVector meta.PlacementVector
		rep := uint8(1)
		if i < len(replicas) {
			rep = uint8(policy.ReplicaNumberByIndex(i))
		}
		cnrVector.REP = rep

		slices.SortFunc(v, func(a, b netmap.NodeInfo) int {
			return bytes.Compare(a.PublicKey(), b.PublicKey())
		})

		kk := make(keys.PublicKeys, 0, len(v))
		for _, n := range v {
			k, err := keys.NewPublicKeyFromBytes(n.PublicKey(), elliptic.P256())
			if err != nil {
				return fmt.Errorf("could not parse public key for %s meta container placement: %w", cID, err)
			}
			kk = append(kk, k)
		}

		sort.Sort(kk)
		cnrVector.Nodes = kk

		placement = append(placement, cnrVector)
	}

	return callMetaContractAndWait(act, nonce, "updateContainerList", cID[:], &placement)
}

func callMetaContractAndWait(act *notary.Actor, nonce uint32, method string, params ...any) error {
	_, err := act.WaitSuccess(act.Notarize(act.MakeTunedCall(meta.Hash, method, nil, func(r *result.Invoke, t *transaction.Transaction) error {
		if r.State != vmstate.Halt.String() {
			return fmt.Errorf("script failed (%s state) due to an error: %s", r.State, r.FaultException)
		}

		vub, err := calculateVUB(act)
		if err != nil {
			return fmt.Errorf("could not calculate vub: %w", err)
		}

		t.ValidUntilBlock = vub
		t.Nonce = nonce

		// Add 10% GAS to prevent this errors:
		// "at instruction 1689 (SYSCALL): System.Runtime.Log failed: insufficient amount of gas"
		t.SystemFee += t.SystemFee / 10

		return nil
	}, params...)))
	if err != nil {
		return fmt.Errorf("notary request invocation : %w", err)
	}
	return err
}

func calculateVUB(cli *notary.Actor) (uint32, error) {
	bc, err := cli.GetBlockCount()
	if err != nil {
		return 0, fmt.Errorf("can't get current blockchain height: %w", err)
	}

	const (
		defaultNotaryValidTime = 50
		defaultNotaryRoundTime = 100
	)

	minIndex := bc + defaultNotaryValidTime
	rounded := (minIndex/defaultNotaryRoundTime + 1) * defaultNotaryRoundTime

	return rounded, nil
}

func enableMetadataChain(ctx context.Context, server *Server, cfg *config.Config, errChan chan<- error) (*metadataNotaryClient, error) {
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
	metaCli, err := server.metaChain.BuildWSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("build meta chain client: %w", err)
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

	return &metadataNotaryClient{
		l:       server.log.With(zap.String("component", "metadata chain alphabet client")),
		metaCli: metaCli,
		indexer: server.statusIndex,
		nodeKey: server.key,
	}, nil
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
