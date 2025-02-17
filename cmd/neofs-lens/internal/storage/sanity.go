package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mr-tron/base58"
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	fstreeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/fstree"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	commonb "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var storageSanityCMD = &cobra.Command{
	Use:   "sanity",
	Short: "Check consistency of stored objects",
	Args:  cobra.NoArgs,
	RunE:  sanityCheck,
}

func init() {
	common.AddConfigFileFlag(storageSanityCMD, &vConfig)
}

type storageShard struct {
	m   *meta.DB
	fsT *fstree.FSTree
	// nolint:staticcheck
	p *peapod.Peapod
}

func sanityCheck(cmd *cobra.Command, _ []string) error {
	var shards []storageShard
	defer func() {
		for _, sh := range shards {
			_ = sh.m.Close()
			if sh.p != nil {
				_ = sh.p.Close()
			}
			if sh.fsT != nil {
				_ = sh.fsT.Close()
			}
		}
	}()

	appCfg := config.New(config.Prm{}, config.WithConfigFile(vConfig))
	err := engineconfig.IterateShards(appCfg, false, func(sc *shardconfig.Config) error {
		var sh storageShard

		blobStorCfg := sc.BlobStor()
		metaCfg := sc.Metabase()

		sh.m = meta.New(
			meta.WithPath(metaCfg.Path()),
			meta.WithPermissions(metaCfg.BoltDB().Perm()),
			meta.WithMaxBatchSize(metaCfg.BoltDB().MaxBatchSize()),
			meta.WithMaxBatchDelay(metaCfg.BoltDB().MaxBatchDelay()),
			meta.WithBoltDBOptions(&bbolt.Options{Timeout: time.Second}),
			meta.WithLogger(zap.NewNop()),
			meta.WithEpochState(epochState{}),
		)

		for _, subCfg := range blobStorCfg.Storages() {
			switch subCfg.Type() {
			default:
				return fmt.Errorf("unsupported sub-storage type '%s'", subCfg.Type())
			case peapod.Type:
				sh.p = peapod.New(subCfg.Path(), subCfg.Perm(), subCfg.FlushInterval())

				var compressCfg compression.Config
				err := compressCfg.Init()
				if err != nil {
					return fmt.Errorf("failed to init compression config: %w", err)
				}

				sh.p.SetCompressor(&compressCfg)
			case fstree.Type:
				fstreeCfg := fstreeconfig.From((*config.Config)(subCfg))
				sh.fsT = fstree.New(
					fstree.WithPath(subCfg.Path()),
					fstree.WithPerm(subCfg.Perm()),
					fstree.WithDepth(fstreeCfg.Depth()),
					fstree.WithNoSync(fstreeCfg.NoSync()),
				)
			}
		}

		if err := sh.m.Open(true); err != nil {
			return fmt.Errorf("open metabase: %w", err)
		}
		if sh.p != nil {
			if err := sh.p.Open(true); err != nil {
				return fmt.Errorf("open peapod: %w", err)
			}
		}
		if sh.fsT != nil {
			if err := sh.fsT.Open(true); err != nil {
				return fmt.Errorf("open fstree: %w", err)
			}
		}

		// metabase.Open(true) does not set it mode to RO somehow
		if err := sh.m.SetMode(mode.ReadOnly); err != nil {
			return fmt.Errorf("moving metabase in readonly mode: %w", err)
		}

		if err := sh.m.Init(); err != nil {
			return fmt.Errorf("init metabase: %w", err)
		}
		if sh.p != nil {
			if err := sh.p.Init(); err != nil {
				return fmt.Errorf("init peapod: %w", err)
			}
		}
		if sh.fsT != nil {
			if err := sh.fsT.Init(); err != nil {
				return fmt.Errorf("init fstree: %w", err)
			}
		}

		shards = append(shards, sh)

		return nil
	})
	if err != nil {
		return fmt.Errorf("reading config: %w", err)
	}

	for _, sh := range shards {
		idRaw, err := sh.m.ReadShardID()
		if err != nil {
			cmd.Printf("reading shard id: %s; skip this shard\n", err)
			continue
		}

		id := base58.Encode(idRaw)
		cmd.Printf("Checking %s shard\n", id)

		objsChecked, err := checkShard(cmd, sh)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			cmd.Printf("%d objects checked in %s shard, interrupted by error: %s\n", objsChecked, id, err)
			continue
		}

		cmd.Printf("Checked objects in %s shard: %d", id, objsChecked)
	}

	return nil
}

func checkShard(cmd *cobra.Command, sh storageShard) (int, error) {
	var (
		addrs          []objectcore.AddressWithType
		cursor         *meta.Cursor
		err            error
		objectsChecked int
	)

	for {
		addrs, cursor, err = sh.m.ListWithCursor(1024, cursor)
		if err != nil {
			if errors.Is(err, meta.ErrEndOfListing) {
				return objectsChecked, nil
			}

			return objectsChecked, fmt.Errorf("listing objects in metabase: %w", err)
		}

		for _, obj := range addrs {
			select {
			case <-cmd.Context().Done():
				return objectsChecked, cmd.Context().Err()
			default:
			}

			addr := obj.Address

			sid, err := sh.m.StorageID(addr)
			if err != nil {
				return objectsChecked, fmt.Errorf("reading %s storage ID in metabase: %w", addr, err)
			}

			header, err := sh.m.Get(addr, false)
			if err != nil {
				return objectsChecked, fmt.Errorf("reading %s object in metabase: %w", addr, err)
			}

			switch id := string(sid); id {
			case "":
				if sh.fsT != nil {
					err = checkObject(*header, sh.fsT)
				} else {
					err = fmt.Errorf("incorrect metabase, no FSTree, but object found")
				}
			case peapod.Type:
				if sh.p != nil {
					err = checkObject(*header, sh.p)
				} else {
					err = fmt.Errorf("incorrect metabase, no Peapod, but object found")
				}
			default:
				err = fmt.Errorf("uknown storage ID: %s", id)
			}

			if err != nil {
				if errors.Is(err, logicerr.Error) {
					cmd.Printf("%s object failed check: %s\n", addr, err)
					continue
				}

				return objectsChecked, fmt.Errorf("critical error at %s object check: %w", addr, err)
			}

			objectsChecked++
		}
	}
}

func checkObject(objHeader object.Object, storage commonb.Storage) error {
	// header len check

	raw := objHeader.Marshal()
	if lenRead := len(raw); lenRead > object.MaxHeaderLen {
		return fmt.Errorf("header cannot be larger than %d bytes, read %d", object.MaxHeaderLen, lenRead)
	}

	// object real presence

	obj, err := storage.Get(objectcore.AddressOf(&objHeader))
	if err != nil {
		return fmt.Errorf("object get from %s storage: %w", storage.Type(), err)
	}

	if !bytes.Equal(raw, obj.CutPayload().Marshal()) {
		return errors.New("object from metabase does not match object from storage")
	}

	return nil
}
