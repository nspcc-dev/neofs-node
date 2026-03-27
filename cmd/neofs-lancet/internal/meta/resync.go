package meta

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal"
	blobstorcommon "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var resyncCMD = &cobra.Command{
	Use:   "resync",
	Short: "Resync metabase with blobstor",
	Long: `Reset the metabase and repopulate it by iterating over all objects
stored in the given blobstor directory.`,
	Args: cobra.NoArgs,
	RunE: resyncFunc,
}

var (
	vBlobstorPath string
	vForce        bool
)

const (
	blobstorFlagName = "blobstor"
	forceFlagName    = "force"
)

func init() {
	resyncCMD.Flags().StringVar(&vBlobstorPath, blobstorFlagName, "", "Path to blobstor directory to resync with")
	err := resyncCMD.MarkFlagRequired(blobstorFlagName)
	if err != nil {
		panic(fmt.Errorf("mark required flag %s failed: %w", blobstorFlagName, err))
	}
	err = resyncCMD.MarkFlagFilename(blobstorFlagName)
	if err != nil {
		panic(fmt.Errorf("mark flag %s as filename failed: %w", blobstorFlagName, err))
	}
	resyncCMD.Flags().BoolVar(&vForce, forceFlagName, false, "Force resync even if metabase shard ID does not match blobstor")
	common.AddComponentPathFlag(resyncCMD, &vPath)
}

func resyncFunc(cmd *cobra.Command, _ []string) error {
	db, err := openMeta(false)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Init(blobstorcommon.ID{})
	if err != nil {
		return fmt.Errorf("init metabase: %w", err)
	}

	fst := fstree.New(
		fstree.WithPath(vBlobstorPath),
		fstree.WithPerm(0600),
	)
	defer fst.Close()

	var compressCfg compression.Config
	err = compressCfg.Init()
	if err != nil {
		return fmt.Errorf("failed to init compression config: %w", err)
	}
	fst.SetCompressor(&compressCfg)

	err = fst.Open(true)
	if err != nil {
		return fmt.Errorf("failed to open FSTree: %w", err)
	}

	err = fst.Init(blobstorcommon.ID{})
	if err != nil {
		return fmt.Errorf("init blobstor: %w", err)
	}

	blobstorShardID := fst.ShardID()
	idRaw, err := db.ReadShardID()
	if err != nil {
		return fmt.Errorf("read shard ID from metabase: %w", err)
	}
	var metaShardID blobstorcommon.ID
	if len(idRaw) != 0 {
		metaShardID, err = blobstorcommon.NewIDFromBytes(idRaw)
		if err != nil {
			return fmt.Errorf("decode metabase shard ID: %w", err)
		}
	}
	if !metaShardID.IsZero() && !metaShardID.Equal(blobstorShardID) && !vForce {
		return fmt.Errorf("metabase shard ID %q does not match blobstor shard ID %q, use --%s to override", metaShardID, blobstorShardID, forceFlagName)
	}

	cmd.Println("Iterating over blobstor objects...")

	err = db.ResyncFromBlobstor(fst, func(addr oid.Address, err error) error {
		cmd.PrintErrf("warn: object %s: %v\n", addr, err)
		return nil
	})
	if err != nil {
		return err
	}

	cmd.Println("Metabase resync completed successfully.")
	return nil
}
