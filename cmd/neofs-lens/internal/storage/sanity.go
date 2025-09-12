package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/bbolt"
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	commonb "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
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
}

func sanityCheck(cmd *cobra.Command, _ []string) error {
	var shards []storageShard
	defer func() {
		for _, sh := range shards {
			_ = sh.m.Close()
			if sh.fsT != nil {
				_ = sh.fsT.Close()
			}
		}
	}()

	appCfg, err := config.New(config.WithConfigFile(vConfig))
	if err != nil {
		return fmt.Errorf("failed to load config file: %w", err)
	}
	err = engineconfig.IterateShards(&appCfg.Storage, false, func(sc *shardconfig.ShardDetails) error {
		var sh storageShard

		metaCfg := sc.Metabase

		sh.m = meta.New(
			meta.WithPath(metaCfg.Path),
			meta.WithPermissions(metaCfg.Perm),
			meta.WithMaxBatchSize(int(metaCfg.MaxBatchSize)),
			meta.WithMaxBatchDelay(metaCfg.MaxBatchDelay),
			meta.WithBoltDBOptions(&bbolt.Options{Timeout: time.Second}),
			meta.WithLogger(zap.NewNop()),
			meta.WithEpochState(epochState{}),
		)

		subCfg := sc.Blobstor
		switch subCfg.Type {
		default:
			return fmt.Errorf("unsupported sub-storage type '%s'", subCfg.Type)
		case fstree.Type:
			sh.fsT = fstree.New(
				fstree.WithPath(subCfg.Path),
				fstree.WithPerm(subCfg.Perm),
				fstree.WithDepth(subCfg.Depth),
				fstree.WithNoSync(*subCfg.NoSync),
			)
		}

		if err := sh.m.Open(true); err != nil {
			return fmt.Errorf("open metabase: %w", err)
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

			header, err := sh.m.Get(addr, false)
			if err != nil {
				return objectsChecked, fmt.Errorf("reading %s object in metabase: %w", addr, err)
			}

			var checkErr error
			if sh.fsT != nil {
				checkErr = checkObject(*header, sh.fsT)
			}

			if checkErr != nil {
				if errors.Is(checkErr, logicerr.Error) {
					cmd.Printf("%s object failed check: %s\n", addr, checkErr)
					continue
				}

				return objectsChecked, fmt.Errorf("critical error at %s object check: %w", addr, checkErr)
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
