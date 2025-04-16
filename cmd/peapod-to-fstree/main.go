package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"gopkg.in/yaml.v3"
)

type onlyObjectsWithContainersStorage struct {
	common.Storage
	containers    map[cid.ID]struct{}
	read, skipped int
}

func (s *onlyObjectsWithContainersStorage) Iterate(objHandler func(oid.Address, []byte, []byte) error, _ func(oid.Address, error) error) error {
	return s.Storage.Iterate(func(addr oid.Address, data []byte, id []byte) error {
		s.read++

		_, found := s.containers[addr.Container()]
		if !found {
			s.skipped++
			return nil
		}

		return objHandler(addr, data, id)
	}, func(addr oid.Address, err error) error {
		_, found := s.containers[addr.Container()]
		if !found {
			log.Printf("skipping corrupted object without container '%s': %v\n", addr, err)
			return nil
		}
		return err
	})
}

func main() {
	nodeCfgPath := flag.String("config", "", "Path to storage node's YAML configuration file")

	flag.Parse()

	if *nodeCfgPath == "" {
		log.Fatal("missing storage node config flag")
	}

	srcPath := *nodeCfgPath

	dstPath := srcPath + ".migrated"

	log.Printf("migrating configuration to '%s' file...\n", dstPath)

	err := migrateConfigToFstree(dstPath, srcPath)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("configuration successfully migrated, migrating data in shards...")

	appCfg, err := config.New(config.WithConfigFile(*nodeCfgPath))
	if err != nil {
		log.Fatal(err)
	}
	cnrs := actualContainers(appCfg)

	i := uint64(0)
	err = engineconfig.IterateShards(&appCfg.Storage, false, func(sc *shardconfig.ShardDetails) error {
		log.Printf("processing shard %d...\n", i)

		var (
			countLimit  int
			ppd, fstr   common.Storage
			storagesCfg = sc.Blobstor
		)

		for _, storageCfg := range storagesCfg {
			switch storageCfg.Type {
			case fstree.Type:
				countLimit = storageCfg.CombinedCountLimit

				fstr = fstree.New(
					fstree.WithPath(storageCfg.Path),
					fstree.WithPerm(storageCfg.Perm),
					fstree.WithDepth(storageCfg.Depth),
					fstree.WithNoSync(*storageCfg.NoSync),
					fstree.WithCombinedCountLimit(countLimit),
					fstree.WithCombinedSizeLimit(int(storageCfg.CombinedSizeLimit)),
					fstree.WithCombinedSizeThreshold(int(storageCfg.CombinedSizeThreshold)),
					fstree.WithCombinedWriteInterval(storageCfg.FlushInterval),
				)

			case peapod.Type:
				ppd = peapod.New(
					storageCfg.Path,
					storageCfg.Perm,
					storageCfg.FlushInterval,
				)
			default:
				return fmt.Errorf("invalid storage type: %s", storageCfg.Type)
			}
		}

		if ppd == nil {
			log.Printf("Peapod is not configured for the shard %d, going to next one...\n", i)
			return nil
		}

		if fstr == nil {
			return fmt.Errorf("FSTree is not configured for the shard %d, please configure some fstree for this shard, going to next one...\n", i)
		}

		ppdPath := ppd.Path()
		if !filepath.IsAbs(ppdPath) {
			log.Fatalf("Peapod path '%s' is not absolute, make it like this in the config file first\n", ppdPath)
		}

		var compressCfg compression.Config
		compressCfg.Enabled = *sc.Compress
		compressCfg.UncompressableContentTypes = sc.CompressionExcludeContentTypes

		err := compressCfg.Init()
		if err != nil {
			log.Fatalf("init compression config for the shard %d: %v", i, err)
		}

		ppd.SetCompressor(&compressCfg)
		fstr.SetCompressor(&compressCfg)

		log.Printf("migrating data from Peapod '%s' to Fstree '%s' (batches of %d objects)...\n", ppd.Path(), fstr.Path(), countLimit)

		onlyActualObjectsPeapod := onlyObjectsWithContainersStorage{Storage: ppd, containers: cnrs}

		err = common.CopyBatched(fstr, &onlyActualObjectsPeapod, countLimit)
		if err != nil {
			log.Fatal("migration failed: ", err)
		}

		log.Printf("data successfully migrated (read objects: %d, skipped: %d) in the shard %d, going to the next one...\n", onlyActualObjectsPeapod.read, onlyActualObjectsPeapod.skipped, i)

		i++
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("data successfully migrated in all shards")
}

func migrateConfigToFstree(dstPath, srcPath string) error {
	fData, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("read source config file: %w", err)
	}

	var mConfig map[any]any

	err = yaml.Unmarshal(fData, &mConfig)
	if err != nil {
		return fmt.Errorf("decode config from YAML: %w", err)
	}

	v, ok := mConfig["storage"]
	if !ok {
		return errors.New("missing 'storage' section")
	}

	mStorage, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("unexpected 'storage' section type: %T instead of %T", v, mStorage)
	}

	v, ok = mStorage["shards"]
	if !ok {
		return errors.New("missing 'storage.shards' section")
	}

	sShards, ok := v.([]any)
	if !ok {
		return fmt.Errorf("unexpected 'storage.shards' section type: %T instead of %T", v, sShards)
	}

	replacePeapodWithFstree := func(mShard map[string]any, shardDesc any) error {
		v, ok := mShard["blobstor"]
		if !ok {
			return fmt.Errorf("missing 'blobstor' section in shard '%v' config", shardDesc)
		}

		sBlobStor, ok := v.([]any)
		if !ok {
			return fmt.Errorf("unexpected 'blobstor' section type in shard '%v': %T instead of %T", shardDesc, v, sBlobStor)
		}

		var ppdSubStorage map[string]any
		var ppdSubStorageIndex int
		var fstreeExist bool

		for i := range sBlobStor {
			mSubStorage, ok := sBlobStor[i].(map[string]any)
			if !ok {
				return fmt.Errorf("unexpected sub-storage #%d type in shard '%v': %T instead of %T", i, shardDesc, v, mStorage)
			}

			v, ok := mSubStorage["type"]
			if ok {
				typ, ok := v.(string)
				if !ok {
					return fmt.Errorf("unexpected type of sub-storage name: %T instead of %T", v, typ)
				}

				if typ == peapod.Type {
					ppdSubStorage = mSubStorage
					ppdSubStorageIndex = i
				}

				if typ == fstree.Type {
					fstreeExist = true
				}

				continue
			}

			// in 'default' section 'type' may be missing

			_, withDepth := mSubStorage["depth"]
			_, withNoSync := mSubStorage["no_sync"]
			_, withCountLimit := mSubStorage["combined_count_limit"]
			_, withSizeLimit := mSubStorage["combined_size_limit"]
			_, withSizeThreshold := mSubStorage["combined_size_threshold"]

			if !(withDepth || withNoSync || withCountLimit || withSizeLimit || withSizeThreshold) {
				ppdSubStorage = mSubStorage
				ppdSubStorageIndex = i
			}
			fstreeExist = true
		}

		if ppdSubStorage == nil {
			log.Printf("peapod is not configured for the shard '%v', skip\n", shardDesc)
			return nil
		}

		if !fstreeExist {
			return fmt.Errorf("fstree is not configured for the shard '%v', please configure some fstree for this shard, skip\n", shardDesc)
		}

		mShard["blobstor"] = slices.Delete(sBlobStor, ppdSubStorageIndex, ppdSubStorageIndex+1)

		return nil
	}

	v, ok = mStorage["shard_defaults"]
	if ok {
		mShard, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected 'storage.shard_defaults' section type: %T instead of %T", v, mShard)
		}

		err = replacePeapodWithFstree(mShard, "default")
		if err != nil {
			return err
		}
	}

	for i := range sShards {
		v = sShards[i]

		mShard, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected 'storage.shards.%d' section type: %T instead of %T", i, v, mStorage)
		}

		err = replacePeapodWithFstree(mShard, i)
		if err != nil {
			return err
		}
	}

	data, err := yaml.Marshal(mConfig)
	if err != nil {
		return fmt.Errorf("encode modified config into YAML: %w", err)
	}

	err = os.WriteFile(dstPath, data, 0o640)
	if err != nil {
		return fmt.Errorf("write resulting config to the destination file: %w", err)
	}

	return nil
}

func actualContainers(appCfg *config.Config) map[cid.ID]struct{} {
	wsEndpts := appCfg.FSChain.Endpoints
	if len(wsEndpts) == 0 {
		log.Fatal("missing fschain endpoints in config")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	pk, err := keys.NewPrivateKey()
	if err != nil {
		log.Fatal(err)
	}

	c, err := client.New(pk, client.WithContext(ctx), client.WithAutoFSChainScope(), client.WithEndpoints(wsEndpts))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	cnrHash, err := c.NNSContractAddress(client.NNSContainerContractName)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("using container contract hash: %s\n", cnrHash)

	cnrCli, err := container.NewFromMorph(c, cnrHash, 0)
	if err != nil {
		log.Fatal(err)
	}

	cnrs, err := cnrCli.List(nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("actual containers number: %d\n", len(cnrs))

	res := make(map[cid.ID]struct{})
	for _, cnr := range cnrs {
		res[cnr] = struct{}{}
	}

	return res
}
