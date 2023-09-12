package meta

import (
	"errors"
	"os"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
)

var writeObjectCMD = &cobra.Command{
	Use:   "put",
	Short: "Put object to metabase",
	Long:  "Put object from file to metabase",
	Args:  cobra.NoArgs,
	Run:   writeObject,
}

func init() {
	common.AddComponentPathFlag(writeObjectCMD, &vPath)
	common.AddInputPathFile(writeObjectCMD, &vInputObj)
}

func writeObject(cmd *cobra.Command, _ []string) {
	db := openMeta(cmd, false)
	defer db.Close()

	err := db.Init()
	common.ExitOnErr(cmd, common.Errf("can't init metabase: %w", err))

	buf, err := os.ReadFile(vInputObj)
	common.ExitOnErr(cmd, common.Errf("unable to read given file: %w", err))

	obj := object.New()
	common.ExitOnErr(cmd, common.Errf("can't unmarshal object from given file: %w", obj.Unmarshal(buf)))

	id, ok := obj.ID()
	if !ok {
		common.ExitOnErr(cmd, errors.New("missing ID in object"))
	}

	cnr, ok := obj.ContainerID()
	if !ok {
		common.ExitOnErr(cmd, errors.New("missing container ID in object"))
	}

	var pPrm meta.PutPrm
	pPrm.SetObject(obj)

	_, err = db.Put(pPrm)
	common.ExitOnErr(cmd, common.Errf("can't put object: %w", err))

	cmd.Printf("[%s] Object successfully stored\n", vInputObj)
	cmd.Printf("  OID: %s\n  CID: %s\n", id, cnr)
}
