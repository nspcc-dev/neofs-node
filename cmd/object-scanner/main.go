package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	engineconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	blobovniczaconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/blobovnicza"
	fstreeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/fstree"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/tzhash/tz"
)

func main() {
	nodeCfgPath := flag.String("config", "", "Path to storage node's YAML configuration file")
	neoFSAPIEndpoint := flag.String("endpoint", "", "Network address of the NeoFS API server")

	flag.Parse()

	if *nodeCfgPath == "" {
		log.Fatal("missing storage node config flag")
	} else if *neoFSAPIEndpoint == "" {
		log.Fatal("missing NeoFS API endpoint")
	}

	c, err := client.New(client.PrmInit{})
	if err != nil {
		log.Fatal("create NeoFS API client: ", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var dialPrm client.PrmDial
	dialPrm.SetServerURI(*neoFSAPIEndpoint)
	dialPrm.SetContext(ctx)

	err = c.Dial(dialPrm)
	if err != nil {
		log.Fatal("NeoFS API client dial: ", err)
	}

	defer c.Close()

	mCnrCache := make(map[cid.ID]bool) // maps container ID to homomorphic checksum requirement

	var obj object.Object

	handleObj := func(addr oid.Address, obj object.Object) error {
		cnrID := addr.Container()

		homoChecksumRequired, ok := mCnrCache[cnrID]
		if !ok {
			cnr, err := c.ContainerGet(ctx, cnrID, client.PrmContainerGet{})
			if err != nil {
				return fmt.Errorf("read container: %w", err)
			}

			homoChecksumRequired = !cnr.IsHomomorphicHashingDisabled()
			mCnrCache[cnrID] = homoChecksumRequired
		}

		if !homoChecksumRequired {
			return ctx.Err()
		}

		cs, csSet := obj.PayloadHomomorphicHash()
		switch {
		case !csSet:
			log.Printf("missing homomorphic payload checksum in object '%s'\n", addr)
		case cs.Type() != checksum.TZ:
			log.Printf("wrong/unsupported type of homomorphic payload checksum in object '%s': %s instead of %s\n",
				addr, cs.Type(), checksum.TZ)
		case len(cs.Value()) != tz.Size:
			log.Printf("invalid/unsupported length of %s homomorphic payload checksum in object '%s': %d instead of %d\n",
				addr, cs.Type(), len(cs.Value()), tz.Size)
		}

		return ctx.Err()
	}

	iterPrm := common.IteratePrm{
		Handler: func(el common.IterationElement) error {
			err := obj.Unmarshal(el.ObjectData)
			if err != nil {
				return fmt.Errorf("decode object from binary: %w", err)
			}

			return handleObj(el.Address, obj)
		},
	}

	appCfg := config.New(config.Prm{}, config.WithConfigFile(*nodeCfgPath))

	err = engineconfig.IterateShards(appCfg, false, func(sc *shardconfig.Config) error {
		wcCfg := sc.WriteCache()

		if wcCfg.Enabled() {
			wc := writecache.New(
				writecache.WithPath(wcCfg.Path()),
				writecache.WithMaxBatchSize(wcCfg.BoltDB().MaxBatchSize()),
				writecache.WithMaxBatchDelay(wcCfg.BoltDB().MaxBatchDelay()),
				writecache.WithMaxObjectSize(wcCfg.MaxObjectSize()),
				writecache.WithSmallObjectSize(wcCfg.SmallObjectSize()),
				writecache.WithFlushWorkersCount(wcCfg.WorkersNumber()),
				writecache.WithMaxCacheSize(wcCfg.SizeLimit()),
				writecache.WithNoSync(wcCfg.NoSync()),
			)

			err := wc.Open(true)
			if err != nil {
				return fmt.Errorf("open write-cache in read-only mode: %w", err)
			}

			defer wc.Close()

			err = wc.Init()
			if err != nil {
				return fmt.Errorf("init write-cache in read-only mode: %w", err)
			}

			var wcIterPrm writecache.IterationPrm
			wcIterPrm.WithHandler(func(data []byte) error {
				err := obj.Unmarshal(data)
				if err != nil {
					return fmt.Errorf("decode object from binary: %w", err)
				}

				return handleObj(objectcore.AddressOf(&obj), obj)
			})

			err = wc.Iterate(wcIterPrm)
			if err != nil {
				return fmt.Errorf("iterate over objects in the write-cache: %w", err)
			}
		}

		blobStorCfg := sc.BlobStor()

		var subs []blobstor.SubStorage
		for _, subCfg := range blobStorCfg.Storages() {
			switch subCfg.Type() {
			default:
				return fmt.Errorf("unsupported sub-storage type '%s'", subCfg.Type())
			case blobovniczatree.Type:
				bbczCfg := blobovniczaconfig.From((*config.Config)(subCfg))
				subs = append(subs, blobstor.SubStorage{
					Storage: blobovniczatree.NewBlobovniczaTree(
						blobovniczatree.WithRootPath(subCfg.Path()),
						blobovniczatree.WithPermissions(subCfg.Perm()),
						blobovniczatree.WithBlobovniczaSize(bbczCfg.Size()),
						blobovniczatree.WithBlobovniczaShallowDepth(bbczCfg.ShallowDepth()),
						blobovniczatree.WithBlobovniczaShallowWidth(bbczCfg.ShallowWidth()),
						blobovniczatree.WithOpenedCacheSize(bbczCfg.OpenedCacheSize())),
					Policy: func(_ *object.Object, data []byte) bool {
						return uint64(len(data)) < sc.SmallSizeLimit()
					},
				})
			case fstree.Type:
				fstreeCfg := fstreeconfig.From((*config.Config)(subCfg))
				subs = append(subs, blobstor.SubStorage{
					Storage: fstree.New(
						fstree.WithPath(subCfg.Path()),
						fstree.WithPerm(subCfg.Perm()),
						fstree.WithDepth(fstreeCfg.Depth()),
						fstree.WithNoSync(fstreeCfg.NoSync())),
					Policy: func(_ *object.Object, data []byte) bool {
						return true
					},
				})
			}
		}

		bs := blobstor.New(
			blobstor.WithStorages(subs),
			blobstor.WithCompressObjects(sc.Compress()),
			blobstor.WithUncompressableContentTypes(sc.UncompressableContentTypes()),
		)

		err := bs.Open(true)
		if err != nil {
			return fmt.Errorf("open blobstor in read-only mode: %w", err)
		}

		defer bs.Close()

		err = bs.Init()
		if err != nil {
			return fmt.Errorf("init blobstor in read-only mode: %w", err)
		}

		_, err = bs.Iterate(iterPrm)
		if err != nil {
			return fmt.Errorf("iterate over objects in the blobstor: %w", err)
		}

		return ctx.Err()
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("all objects have been checked")
}
