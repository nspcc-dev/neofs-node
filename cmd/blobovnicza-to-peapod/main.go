package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	blobovniczaconfig "github.com/nspcc-dev/neofs-node/cmd/blobovnicza-to-peapod/bbczconf"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"gopkg.in/yaml.v3"
)

func main() {
	nodeCfgPath := flag.String("config", "", "Path to storage node's YAML configuration file")

	flag.Parse()

	if *nodeCfgPath == "" {
		log.Fatal("missing storage node config flag")
	}

	appCfg := config.New(config.Prm{}, config.WithConfigFile(*nodeCfgPath))

	err := engineconfig.IterateShards(appCfg, false, func(sc *shardconfig.Config) error {
		log.Println("processing shard...")

		var bbcz common.Storage
		var perm fs.FileMode
		storagesCfg := sc.BlobStor().Storages()

		for i := range storagesCfg {
			if storagesCfg[i].Type() == blobovniczatree.Type {
				bbczCfg := blobovniczaconfig.From((*config.Config)(storagesCfg[i]))

				perm = storagesCfg[i].Perm()
				bbcz = blobovniczatree.NewBlobovniczaTree(
					blobovniczatree.WithRootPath(storagesCfg[i].Path()),
					blobovniczatree.WithPermissions(storagesCfg[i].Perm()),
					blobovniczatree.WithBlobovniczaSize(bbczCfg.Size()),
					blobovniczatree.WithBlobovniczaShallowDepth(bbczCfg.ShallowDepth()),
					blobovniczatree.WithBlobovniczaShallowWidth(bbczCfg.ShallowWidth()),
					blobovniczatree.WithOpenedCacheSize(bbczCfg.OpenedCacheSize()))

				break
			}
		}

		if bbcz == nil {
			log.Println("Blobovnicza is not configured for the current shard, going to next one...")
			return nil
		}

		bbczPath := bbcz.Path()
		if !filepath.IsAbs(bbczPath) {
			log.Fatalf("Blobobvnicza tree path '%s' is not absolute, make it like this in the config file first\n", bbczPath)
		}

		ppdPath := filepath.Join(filepath.Dir(bbcz.Path()), "peapod.db")
		ppd := peapod.New(ppdPath, perm, 10*time.Millisecond)

		var compressCfg compression.Config
		compressCfg.Enabled = sc.Compress()
		compressCfg.UncompressableContentTypes = sc.UncompressableContentTypes()

		err := compressCfg.Init()
		if err != nil {
			log.Fatal("init compression config for the current shard: ", err)
		}

		bbcz.SetCompressor(&compressCfg)
		ppd.SetCompressor(&compressCfg)

		log.Printf("migrating data from Blobovnicza tree '%s' to Peapod '%s'...\n", bbcz.Path(), ppd.Path())

		err = common.Copy(ppd, bbcz)
		if err != nil {
			log.Fatal("migration failed: ", err)
		}

		log.Println("data successfully migrated in the current shard, going to the next one...")

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	srcPath := *nodeCfgPath
	ss := strings.Split(srcPath, ".")
	ss[0] += "_peapod"

	dstPath := strings.Join(ss, ".")

	log.Printf("data successfully migrated in all shards, migrating configuration to '%s' file...\n", dstPath)

	err = migrateConfigToPeapod(dstPath, srcPath)
	if err != nil {
		log.Fatal(err)
	}
}

func migrateConfigToPeapod(dstPath, srcPath string) error {
	fData, err := os.ReadFile(srcPath)
	if err != nil {
		return fmt.Errorf("read source config config file: %w", err)
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

	replaceBlobovniczaWithPeapod := func(mShard map[string]any, shardDesc any) error {
		v, ok := mShard["blobstor"]
		if !ok {
			return fmt.Errorf("missing 'blobstor' section in shard '%v' config", shardDesc)
		}

		sBlobStor, ok := v.([]any)
		if !ok {
			return fmt.Errorf("unexpected 'blobstor' section type in shard '%v': %T instead of %T", shardDesc, v, sBlobStor)
		}

		var bbczSubStorage map[string]any

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

				if typ == blobovniczatree.Type {
					bbczSubStorage = mSubStorage
				}

				continue
			}

			// in 'default' section 'type' may be missing

			_, withDepth := mSubStorage["depth"]
			_, withWidth := mSubStorage["width"]

			if withWidth && withDepth {
				bbczSubStorage = mSubStorage
			}
		}

		if bbczSubStorage == nil {
			log.Printf("blobovnicza tree is not configured for the shard '%s', skip\n", shardDesc)
			return nil
		}

		for k := range bbczSubStorage {
			switch k {
			default:
				delete(bbczSubStorage, k)
			case "type", "path", "perm":
			}
		}

		bbczSubStorage["type"] = peapod.Type

		v, ok = bbczSubStorage["path"]
		if ok {
			path, ok := v.(string)
			if !ok {
				return fmt.Errorf("unexpected sub-storage path type: %T instead of %T", v, path)
			}

			bbczSubStorage["path"] = filepath.Join(filepath.Dir(path), "peapod.db")
		}

		return nil
	}

	v, ok = mShards["default"]
	if ok {
		mShard, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("unexpected 'storage.shard.default' section type: %T instead of %T", v, mShard)
		}

		err = replaceBlobovniczaWithPeapod(mShard, "default")
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

		err = replaceBlobovniczaWithPeapod(mShard, i)
		if err != nil {
			return err
		}
	}

	data, err := yaml.Marshal(mConfig)
	if err != nil {
		return fmt.Errorf("encode modified config into YAML: %w", err)
	}

	err = os.WriteFile(dstPath, data, 0640)
	if err != nil {
		return fmt.Errorf("write resulting config to the destination file: %w", err)
	}

	return nil
}
