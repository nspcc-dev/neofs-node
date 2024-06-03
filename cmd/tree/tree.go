package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	netmapV2 "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	netmapGRPC "github.com/nspcc-dev/neofs-api-go/v2/netmap/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/message"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	treeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/tree"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	statusSDK "github.com/nspcc-dev/neofs-sdk-go/client/status"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	aclsdk "github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type metaMock struct {
	cnr    *container.Container
	netmap *netmap.NetMap
}

func (e metaMock) GetNetMap(_ uint64) (*netmap.NetMap, error) {
	return e.netmap, nil
}

func (e metaMock) GetNetMapByEpoch(_ uint64) (*netmap.NetMap, error) {
	panic("why you call me?")
}

func (e metaMock) Epoch() (uint64, error) {
	panic("why you call me?")
}

func (e metaMock) Get(_ cid.ID) (*container.Container, error) {
	return e.cnr, nil
}

func (e metaMock) List() ([]cid.ID, error) {
	return []cid.ID{{}}, nil
}

func (e metaMock) GetEACL(id cid.ID) (*container.EACL, error) {
	// no restricted eACLs for load testing
	return nil, statusSDK.ErrEACLNotFound
}

func runTree() {
	configFile := flag.String("config", "", "path to config")
	keyFile := flag.String("key", "", "path to private key")
	nemtapFile := flag.String("netmap", "", "path to netmap file")
	listenOn := flag.String("listen", "", "endpoint to listen on")
	flag.Parse()

	panicIfEmpty(configFile, "empty config file path")
	panicIfEmpty(nemtapFile, "empty nemtap file path")
	panicIfEmpty(keyFile, "empty key file path")
	panicIfEmpty(listenOn, "empty endpoint")

	rawKey, err := os.ReadFile(*keyFile)
	if err != nil {
		panic(fmt.Sprintf("failed to read key file: %s", err))
	}

	key, err := keys.NewPrivateKeyFromBytes(rawKey)
	if err != nil {
		panic(fmt.Sprintf("failed to parse private key: %s", err))
	}

	rawNetmap, err := os.ReadFile(*nemtapFile)
	if err != nil {
		panic(fmt.Sprintf("failed to read netmap file: %s", err))
	}

	var nmV2 netmapV2.NetMap
	err = message.UnmarshalJSON(&nmV2, rawNetmap, new(netmapGRPC.Netmap))
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal netmap from file: %s", err))
	}

	var nm netmap.NetMap
	err = nm.ReadFromV2(nmV2)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal netmap from its V2 version: %s", err))
	}

	numOfNodes := len(nmV2.Nodes())

	var policy netmap.PlacementPolicy
	err = policy.DecodeString(fmt.Sprintf("REP %d", numOfNodes))
	if err != nil {
		panic(fmt.Sprintf("failed to parse hardcoded policy: %s", err))
	}

	var cnrSDK containerSDK.Container
	cnrSDK.SetBasicACL(aclsdk.PublicRW)
	cnrSDK.SetOwner(user.NewAutoIDSignerRFC6979(key.PrivateKey).UserID())
	cnrSDK.SetPlacementPolicy(policy)

	appCfg := config.New(config.Prm{}, config.WithConfigFile(*configFile))
	treeCfg := treeconfig.Tree(appCfg)
	piloramaCfg := shardconfig.From(appCfg).Pilorama()
	metaInfo := metaMock{
		cnr: &container.Container{
			Value: cnrSDK,
		},
		netmap: &nm,
	}

	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	forest := pilorama.NewBoltForest(
		pilorama.WithPath(piloramaCfg.Path()),
		pilorama.WithPerm(piloramaCfg.Perm()),
		pilorama.WithNoSync(piloramaCfg.NoSync()),
		pilorama.WithMaxBatchSize(piloramaCfg.MaxBatchSize()),
		pilorama.WithMaxBatchDelay(piloramaCfg.MaxBatchDelay()),
	)

	err = forest.Open(false)
	if err != nil {
		panic(err)
	}
	err = forest.Init()
	if err != nil {
		panic(err)
	}

	t := tree.New(
		tree.WithContainerSource(metaInfo),
		tree.WithEACLSource(metaInfo),
		tree.WithNetmapSource(metaInfo),
		tree.WithPrivateKey(&key.PrivateKey),
		tree.WithLogger(l),
		tree.WithStorage(forest),
		tree.WithContainerCacheSize(treeCfg.CacheSize()),
		tree.WithReplicationTimeout(treeCfg.ReplicationTimeout()),
		tree.WithReplicationChannelCapacity(treeCfg.ReplicationChannelCapacity()),
		tree.WithReplicationWorkerCount(treeCfg.ReplicationWorkerCount()),
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	d := treeCfg.SyncInterval()
	ticker := time.NewTicker(d)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := t.SynchronizeAll()
				if err != nil {
					l.Error("syncing trees", zap.Error(err))
				}
			}
		}
	}()

	t.Start(ctx)
	serveTreeService(ctx, t, *listenOn)
}

func serveTreeService(ctx context.Context, t *tree.Service, endpoint string) {
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		panic(fmt.Errorf("listen on %s failed: %w", endpoint, err))
	}

	// consts are taken from the real nodes
	const maxRcvMsg = 64<<20 /*max obj payload size*/ + object.MaxHeaderLen /*header size limit*/
	const maxMsgSize = 4 << 20

	srv := grpc.NewServer(grpc.MaxRecvMsgSize(maxRcvMsg), grpc.MaxSendMsgSize(maxMsgSize))
	tree.RegisterTreeServiceServer(srv, t)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	err = srv.Serve(lis)
	if err != nil {
		fmt.Printf("serve failed: %s\n", err)
	}
}

func panicIfEmpty(v *string, message string) {
	if v == nil || *v == "" {
		panic(message)
	}
}
