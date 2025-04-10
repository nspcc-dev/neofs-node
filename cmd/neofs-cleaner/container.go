package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"time"

	"github.com/nspcc-dev/neo-go/cli/input"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	fstreeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/fstree"
	fschainconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/fschain"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

type epochState struct{}

func (e epochState) CurrentEpoch() uint64 {
	return 0
}

func exitOnError(err error) {
	if err != nil {
		fmt.Printf("fatal: %s\n", err)
		os.Exit(1)
	}
}

const (
	networkCnrsFile = "network_containers.txt"
	readCnrsFile    = "storage_containers.txt"
	garbageCnrsFile = "garbage_containers.txt"
)

type sh struct {
	m   *meta.DB
	p   *peapod.Peapod
	fst *fstree.FSTree
}

func writeContainersToFile(fileName string, cc map[cid.ID]struct{}) error {
	f, err := os.OpenFile(fileName, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	exitOnError(err)
	defer func() {
		_ = f.Close()
	}()

	for cID := range cc {
		_, err = f.WriteString(cID.EncodeToString() + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	nodeCfgPath := flag.String("config", "", "Path to storage node's YAML configuration file")
	dryRun := flag.Bool("dry-run", false, "Do not change storages")
	flag.Parse()
	if *nodeCfgPath == "" {
		exitOnError(errors.New("missing storage node config flag"))
	}
	fmt.Printf("using %q storage config path...\n", *nodeCfgPath)
	if *dryRun {
		fmt.Println("\n=== DRY RUN MODE ===\n")
	} else {
		fmt.Println("\n=== STORAGES WILL BE CHANGED ===\n")
	}

	appCfg := config.New(config.Prm{}, config.WithConfigFile(*nodeCfgPath))

	wsEndpts := fschainconfig.Endpoints(appCfg)
	if len(wsEndpts) == 0 {
		exitOnError(errors.New("missing fschain endpoints in config"))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	pk, err := keys.NewPrivateKey()
	exitOnError(err)

	c, err := client.New(pk, client.WithContext(ctx), client.WithAutoFSChainScope(), client.WithEndpoints(wsEndpts))
	exitOnError(err)
	cnrHash, err := c.NNSContractAddress(client.NNSContainerContractName)
	exitOnError(err)

	fmt.Printf("using container contract hash: %s\n", cnrHash)

	cnrCli, err := container.NewFromMorph(c, cnrHash, 0)
	exitOnError(err)

	cnrs, err := cnrCli.List(nil)
	exitOnError(err)
	networkContainersM := sliceToMap(cnrs)

	err = writeContainersToFile(networkCnrsFile, networkContainersM)
	exitOnError(err)
	fmt.Printf("saved actual containers to %q file\n", networkCnrsFile)
	continuePrompt()

	var shards []sh
	err = engineconfig.IterateShards(appCfg, true, func(s *shardconfig.Config) error {
		var shard sh

		var compressCfg compression.Config
		compressCfg.Enabled = s.Compress()
		compressCfg.UncompressableContentTypes = s.UncompressableContentTypes()
		err := compressCfg.Init()
		if err != nil {
			return fmt.Errorf("init compression config for the shard: %w", err)
		}

		mCfg := s.Metabase()
		bCfg := mCfg.BoltDB()
		shard.m = meta.New(meta.WithPath(mCfg.Path()),
			meta.WithEpochState(epochState{}),
			meta.WithPermissions(bCfg.Perm()),
			meta.WithBoltDBOptions(&bbolt.Options{
				Timeout: time.Second,
			}),
			meta.WithInitContext(ctx),
		)

		stCfg := s.BlobStor().Storages()
		for _, st := range stCfg {
			switch st.Type() {
			case peapod.Type:
				shard.p = peapod.New(st.Path(), st.Perm(), st.FlushInterval())
				shard.p.SetCompressor(&compressCfg)
			case fstree.Type:
				fCfg := fstreeconfig.From((*config.Config)(st))

				shard.fst = fstree.New(fstree.WithPath(st.Path()),
					fstree.WithPerm(st.Perm()),
					fstree.WithDepth(fCfg.Depth()),
					fstree.WithCombinedCountLimit(fCfg.CombinedCountLimit()),
					fstree.WithCombinedSizeLimit(fCfg.CombinedSizeLimit()),
					fstree.WithCombinedSizeThreshold(fCfg.CombinedSizeThreshold()),
					fstree.WithCombinedWriteInterval(st.FlushInterval()))
				shard.fst.SetCompressor(&compressCfg)
			default:
				return fmt.Errorf("unknown storage type %s", st.Type())
			}
		}

		switch {
		case shard.m == nil:
			return fmt.Errorf("missing meta configuration")
		case shard.fst == nil:
			return fmt.Errorf("missing fstree configuration")
		case shard.p == nil:
			return fmt.Errorf("missing peapod configuration")
		default:
			shards = append(shards, shard)
			return nil
		}
	})
	exitOnError(err)
	fmt.Printf("found %d shards\n", len(shards))

	for _, shard := range shards {
		exitOnError(shard.m.Open(*dryRun))
		exitOnError(shard.p.Open(*dryRun))
		exitOnError(shard.p.Init())
		exitOnError(shard.fst.Open(*dryRun))
	}
	defer func() {
		for _, shard := range shards {
			_ = shard.m.Close()
			_ = shard.p.Close()
			_ = shard.fst.Close()
		}
	}()

	readContainersM := make(map[cid.ID]struct{})
	for _, shard := range shards {
		cc, err := shard.m.Containers()
		exitOnError(err)

		maps.Copy(readContainersM, sliceToMap(cc))
	}

	err = writeContainersToFile(readCnrsFile, readContainersM)
	exitOnError(err)
	fmt.Printf("saved containers from storage to %q file\n", readCnrsFile)

	cnrsToDelete := make(map[cid.ID]struct{})
	for cnrFromStorage := range readContainersM {
		_, ok := networkContainersM[cnrFromStorage]
		if !ok {
			cnrsToDelete[cnrFromStorage] = struct{}{}
		}
	}

	err = writeContainersToFile(garbageCnrsFile, cnrsToDelete)
	exitOnError(err)
	fmt.Printf("saved containers-to-delete to %q file (%d garbage containers)\n", garbageCnrsFile, len(cnrsToDelete))
	continuePrompt()
	fmt.Println()

	for _, sh := range shards {
		err = deleteContainersFromShard(ctx, sh, cnrsToDelete, *dryRun)
		exitOnError(err)
		fmt.Println()

		select {
		case <-ctx.Done():
			exitOnError(ctx.Err())
		default:
		}
	}
}

const msgUpdateTime = 100 * time.Millisecond

func udpateOutput(t *time.Timer, msg string) {
	select {
	case <-t.C:
		fmt.Print(msg)
		t.Reset(msgUpdateTime)
	default:
	}
}

func deleteContainersFromShard(ctx context.Context, shard sh, garbageCnrs map[cid.ID]struct{}, dryRun bool) error {
	info := shard.m.DumpInfo()
	fmt.Printf("handling shard with %q metabase...\n", info.Path)

	t := time.NewTimer(msgUpdateTime)
	var objsToDelete []oid.Address

	var peapodCounter int
	message := func() string {
		return fmt.Sprintf("\rread peapod objects: %d; garbage objects: %d", peapodCounter, len(objsToDelete))
	}
	fmt.Println("cleaning objects in peapod")
	err := shard.p.IterateAddresses(func(addr oid.Address) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		peapodCounter++

		_, ok := garbageCnrs[addr.Container()]
		if ok {
			objsToDelete = append(objsToDelete, addr)
		}

		udpateOutput(t, message())

		return nil
	}, false)
	if err != nil {
		return fmt.Errorf("iterate peapod objects: %w", err)
	}
	if peapodCounter > 0 {
		fmt.Print(message())
		fmt.Println()
	}
	if !dryRun {
		fmt.Printf("will delete %d objects in peapod...\n", len(objsToDelete))

		peapodCounter = 0
		message = func() string {
			return fmt.Sprintf("\rdeleted peapod objects: %d", peapodCounter)
		}
		for _, addr := range objsToDelete {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			err = shard.p.Delete(addr)
			if err != nil {
				return fmt.Errorf("deleted %s object from peapod: %w", addr, err)
			}
			peapodCounter++
			udpateOutput(t, message())
		}
		if peapodCounter > 0 {
			fmt.Print(message())
			fmt.Println()
		}
	}

	if len(objsToDelete) > 0 {
		objsToDelete = objsToDelete[:0]
	}

	var fstCounter int
	message = func() string {
		return fmt.Sprintf("\rread fs tree objects: %d; garbage objects: %d", fstCounter, len(objsToDelete))
	}
	fmt.Println("cleaning objects in fs tree...")
	err = shard.fst.IterateAddresses(func(addr oid.Address) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fstCounter++
		_, ok := garbageCnrs[addr.Container()]
		if ok {
			objsToDelete = append(objsToDelete, addr)
		}

		udpateOutput(t, message())

		return nil
	}, false)
	if err != nil {
		return fmt.Errorf("iterate fs tree objects: %w", err)
	}
	if fstCounter > 0 {
		fmt.Print(message())
		fmt.Println()
	}
	if !dryRun {
		fmt.Printf("will delete %d objects in fs tree...\n", len(objsToDelete))

		fstCounter = 0
		message = func() string {
			return fmt.Sprintf("\rdeleted fs tree objects: %d", fstCounter)
		}
		for _, addr := range objsToDelete {
			err = shard.fst.Delete(addr)
			if err != nil {
				return fmt.Errorf("delete %s object from fs tree: %w", addr, err)
			}

			fstCounter++
			udpateOutput(t, message())
		}
		if fstCounter > 0 {
			fmt.Print(message())
			fmt.Println()
		}
	}

	if !dryRun {
		fmt.Println("delete containers from metabase...")
		var metaCounter int
		message = func() string {
			return fmt.Sprintf("\rdeleted containers from metabase: %d", metaCounter)
		}
		for cnr := range garbageCnrs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			err := shard.m.DeleteContainer(cnr)
			if err != nil {
				return fmt.Errorf("delete %s container: %w", cnr, err)
			}

			metaCounter++
			udpateOutput(t, message())
		}
		if metaCounter > 0 {
			fmt.Print(message())
			fmt.Println()
		}
	}

	return nil
}

func continuePrompt() {
	answr, err := input.ReadLine("continue[y|n]? > ")
	exitOnError(err)

	switch answr {
	case "y", "Y", "yes", "Yes", "YES":
		return
	default:
		exitOnError(errors.New("interupted by user input"))
	}
}

func sliceToMap(cc []cid.ID) map[cid.ID]struct{} {
	res := make(map[cid.ID]struct{})
	for _, c := range cc {
		res[c] = struct{}{}
	}

	return res
}
