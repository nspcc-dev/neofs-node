package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	fstreeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/fstree"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"go.etcd.io/bbolt"
	"gopkg.in/yaml.v3"
)

type epochState struct{}

func (s epochState) CurrentEpoch() uint64 {
	return 0
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

	appCfg := config.New(config.Prm{}, config.WithConfigFile(*nodeCfgPath))

	i := uint64(0)
	err = engineconfig.IterateShards(appCfg, false, func(sc *shardconfig.Config) error {
		log.Printf("processing shard %d...\n", i)

		var ppd, fstr common.Storage
		storagesCfg := sc.BlobStor().Storages()

		for i := range storagesCfg {
			switch storagesCfg[i].Type() {
			case fstree.Type:
				sub := fstreeconfig.From((*config.Config)(storagesCfg[i]))

				fstr = fstree.New(
					fstree.WithPath(storagesCfg[i].Path()),
					fstree.WithPerm(storagesCfg[i].Perm()),
					fstree.WithDepth(sub.Depth()),
					fstree.WithNoSync(sub.NoSync()),
					fstree.WithCombinedCountLimit(sub.CombinedCountLimit()),
					fstree.WithCombinedSizeLimit(sub.CombinedSizeLimit()),
					fstree.WithCombinedSizeThreshold(sub.CombinedSizeThreshold()),
					fstree.WithCombinedWriteInterval(storagesCfg[i].FlushInterval()),
				)

			case peapod.Type:
				ppd = peapod.New(
					storagesCfg[i].Path(),
					storagesCfg[i].Perm(),
					storagesCfg[i].FlushInterval(),
				)
			default:
				return fmt.Errorf("invalid storage type: %s", storagesCfg[i].Type())
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
		compressCfg.Enabled = sc.Compress()
		compressCfg.UncompressableContentTypes = sc.UncompressableContentTypes()

		err := compressCfg.Init()
		if err != nil {
			log.Fatalf("init compression config for the shard %d: %v", i, err)
		}

		ppd.SetCompressor(&compressCfg)
		fstr.SetCompressor(&compressCfg)

		log.Printf("migrating data from Peapod '%s' to Fstree '%s'...\n", ppd.Path(), fstr.Path())

		err = common.Copy(fstr, ppd)
		if err != nil {
			log.Fatal("migration failed: ", err)
		}

		log.Println("updating metabase indexes...")

		readOnly := false
		metabase := meta.New(
			meta.WithPath(sc.Metabase().Path()),
			meta.WithBoltDBOptions(&bbolt.Options{
				ReadOnly: readOnly,
				Timeout:  time.Second,
			}),
			meta.WithEpochState(epochState{}),
		)
		if err := metabase.Open(readOnly); err != nil {
			return fmt.Errorf("could not open metabase in shard %d: %w", i, err)
		}

		var cursor *meta.Cursor
		var addrs []objectcore.AddressWithType
		for {
			addrs, cursor, err = metabase.ListWithCursor(1024, cursor)
			if err != nil {
				if errors.Is(err, meta.ErrEndOfListing) {
					break
				}

				return fmt.Errorf("listing objects in metabase: %w", err)
			}

			for _, obj := range addrs {
				addr := obj.Address

				storageId, err := metabase.StorageID(addr)
				if err != nil {
					return fmt.Errorf("could not get storage id for address %s: %w", addr, err)
				}

				if bytes.Equal(storageId, []byte("peapod")) {
					err = metabase.UpdateStorageID(addr, []byte{})
					if err != nil {
						return fmt.Errorf("could not update storage id for address %s: %w", addr, err)
					}
				}
			}
		}

		log.Printf("data successfully migrated in the shard %d, going to the next one...\n", i)

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

	v, ok = mStorage["shard"]
	if !ok {
		return errors.New("missing 'storage.shard' section")
	}

	mShards, ok := v.(map[any]any)
	if !ok {
		return fmt.Errorf("unexpected 'storage.shard' section type: %T instead of %T", v, mShards)
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

	v, ok = mShards["default"]
	if ok {
		mShard, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected 'storage.shard.default' section type: %T instead of %T", v, mShard)
		}

		err = replacePeapodWithFstree(mShard, "default")
		if err != nil {
			return err
		}
	}

	for i := 0; ; i++ {
		v, ok = mShards[i]
		if !ok {
			if i == 0 {
				return errors.New("missing numbered shards")
			}
			break
		}

		mShard, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected 'storage.shard.%d' section type: %T instead of %T", i, v, mStorage)
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
