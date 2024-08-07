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
	peapodconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/peapod"
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
	Run:   sanityCheck,
}

func init() {
	common.AddConfigFileFlag(storageSanityCMD, &vConfig)
}

type storageShard struct {
	m   *meta.DB
	fsT *fstree.FSTree
	p   *peapod.Peapod
}

func sanityCheck(cmd *cobra.Command, _ []string) {
	var shards []storageShard
	defer func() {
		for _, sh := range shards {
			_ = sh.m.Close()
			_ = sh.p.Close()
			_ = sh.fsT.Close()
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
				peapodCfg := peapodconfig.From((*config.Config)(subCfg))
				sh.p = peapod.New(subCfg.Path(), subCfg.Perm(), peapodCfg.FlushInterval())

				var compressCfg compression.Config
				err := compressCfg.Init()
				common.ExitOnErr(cmd, common.Errf("failed to init compression config: %w", err))

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

		common.ExitOnErr(cmd, common.Errf("open metabase: %w", sh.m.Open(true)))
		common.ExitOnErr(cmd, common.Errf("open peapod: %w", sh.p.Open(true)))
		common.ExitOnErr(cmd, common.Errf("open fstree: %w", sh.fsT.Open(true)))

		// metabase.Open(true) does not set it mode to RO somehow
		common.ExitOnErr(cmd, common.Errf("moving metabase in readonly mode", sh.m.SetMode(mode.ReadOnly)))

		common.ExitOnErr(cmd, common.Errf("init metabase: %w", sh.m.Init()))
		common.ExitOnErr(cmd, common.Errf("init peapod: %w", sh.p.Init()))
		common.ExitOnErr(cmd, common.Errf("init fstree: %w", sh.fsT.Init()))

		shards = append(shards, sh)

		return nil
	})
	common.ExitOnErr(cmd, common.Errf("reading config: %w", err))

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
				return
			}

			cmd.Printf("%d objects checked in %s shard, interrupted by error: %s\n", objsChecked, id, err)
			continue
		}

		cmd.Printf("Checked objects in %s shard: %d", id, objsChecked)
	}
}

func checkShard(cmd *cobra.Command, sh storageShard) (int, error) {
	var objectsChecked int
	var mPrm meta.ListPrm
	mPrm.SetCount(1024)

	for {
		listRes, err := sh.m.ListWithCursor(mPrm)
		if err != nil {
			if errors.Is(err, meta.ErrEndOfListing) {
				return objectsChecked, nil
			}

			return objectsChecked, fmt.Errorf("listing objects in metabase: %w", err)
		}

		for _, obj := range listRes.AddressList() {
			select {
			case <-cmd.Context().Done():
				return objectsChecked, cmd.Context().Err()
			default:
			}

			addr := obj.Address

			var sIDPrm meta.StorageIDPrm
			sIDPrm.SetAddress(addr)

			sIDRes, err := sh.m.StorageID(sIDPrm)
			if err != nil {
				return objectsChecked, fmt.Errorf("reading %s storage ID in metabase: %w", addr, err)
			}

			var mGet meta.GetPrm
			mGet.SetAddress(addr)

			getRes, err := sh.m.Get(mGet)
			if err != nil {
				return objectsChecked, fmt.Errorf("reading %s object in metabase: %w", addr, err)
			}

			header := *getRes.Header()

			switch id := string(sIDRes.StorageID()); id {
			case "":
				err = checkObject(header, sh.fsT)
			case peapod.Type:
				err = checkObject(header, sh.p)
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

		mPrm.SetCursor(listRes.Cursor())
	}
}

func checkObject(objHeader object.Object, storage commonb.Storage) error {
	// header len check

	raw, err := objHeader.Marshal()
	if err != nil {
		return fmt.Errorf("object from metabase cannot be marshaled: %w", err)
	}

	if lenRead := len(raw); lenRead > object.MaxHeaderLen {
		return fmt.Errorf("header cannot be larger than %d bytes, read %d", object.MaxHeaderLen, lenRead)
	}

	// object real presence

	var getPrm commonb.GetPrm
	getPrm.Address = objectcore.AddressOf(&objHeader)

	res, err := storage.Get(getPrm)
	if err != nil {
		return fmt.Errorf("object get from %s storage: %w", storage.Type(), err)
	}

	storageRaw, err := res.Object.CutPayload().Marshal()
	if err != nil {
		return fmt.Errorf("object from %s storage cannot be marshaled: %w", storage.Type(), err)
	}

	if !bytes.Equal(raw, storageRaw) {
		return errors.New("object from metabase does not match object from storage")
	}

	return nil
}
