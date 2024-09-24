package meta

import (
	"errors"
	"fmt"
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
	RunE:  writeObject,
}

func init() {
	common.AddComponentPathFlag(writeObjectCMD, &vPath)
	common.AddInputPathFile(writeObjectCMD, &vInputObj)
}

func writeObject(cmd *cobra.Command, _ []string) error {
	db, err := openMeta(false)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Init()
	if err != nil {
		return fmt.Errorf("can't init metabase: %w", err)
	}

	buf, err := os.ReadFile(vInputObj)
	if err != nil {
		return fmt.Errorf("unable to read given file: %w", err)
	}

	obj := object.New()
	if err := obj.Unmarshal(buf); err != nil {
		return fmt.Errorf("can't unmarshal object from given file: %w", err)
	}

	id, ok := obj.ID()
	if !ok {
		return errors.New("missing ID in object")
	}

	cnr, ok := obj.ContainerID()
	if !ok {
		return errors.New("missing container ID in object")
	}

	var pPrm meta.PutPrm
	pPrm.SetObject(obj)

	_, err = db.Put(pPrm)
	if err != nil {
		return fmt.Errorf("can't put object: %w", err)
	}

	cmd.Printf("[%s] Object successfully stored\n", vInputObj)
	cmd.Printf("  OID: %s\n  CID: %s\n", id, cnr)

	return nil
}
