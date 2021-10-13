package inspect

import (
	"fmt"
	"io/ioutil"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/spf13/cobra"
)

const (
	flagAddress    = "address"
	flagEnginePath = "path"
	flagHeader     = "header"
	flagOutFile    = "out"
	flagWriteCache = "writecache"
)

var (
	vAddress    string
	vHeader     bool
	vPath       string
	vOut        string
	vWriteCache bool
)

// Command contains `inspect` command definition.
var Command = &cobra.Command{
	Use:   "inspect",
	Short: "Object inspection",
	Long:  `Inspect specific object in storage engine component.`,
	Run:   objectInspectCmd,
}

func init() {
	Command.Flags().StringVar(&vAddress, flagAddress, "", "Object address")
	_ = Command.MarkFlagRequired(flagAddress)

	Command.Flags().StringVar(&vPath, flagEnginePath, "",
		"Path to storage engine component",
	)
	_ = Command.MarkFlagFilename(flagEnginePath)
	_ = Command.MarkFlagRequired(flagEnginePath)

	Command.Flags().StringVar(&vOut, flagOutFile, "",
		"File to save object payload")

	Command.Flags().BoolVar(&vHeader, flagHeader, false, "Inspect only header")
	Command.Flags().BoolVar(&vWriteCache, flagWriteCache, false,
		"Process write-cache")
}

func objectInspectCmd(cmd *cobra.Command, _ []string) {
	addr := object.NewAddress()
	err := addr.Parse(vAddress)
	common.ExitOnErr(cmd, common.Errf("invalid address argument: %w", err))

	if vOut == "" && !vHeader {
		common.ExitOnErr(cmd, fmt.Errorf("either --%s or --%s flag must be provided",
			flagHeader, flagOutFile))
	}

	if vWriteCache {
		db, err := writecache.OpenDB(vPath, true)
		common.ExitOnErr(cmd, common.Errf("could not open write-cache db: %w", err))

		defer db.Close()

		data, err := writecache.Get(db, []byte(addr.String()))
		common.ExitOnErr(cmd, common.Errf("could not fetch object: %w", err))
		printObjectInfo(cmd, data)
		return
	}

	blz := blobovnicza.New(
		blobovnicza.WithPath(vPath),
		blobovnicza.ReadOnly())
	common.ExitOnErr(cmd, blz.Open())

	defer blz.Close()

	prm := new(blobovnicza.GetPrm)
	prm.SetAddress(addr)
	res, err := blz.Get(prm)
	common.ExitOnErr(cmd, common.Errf("could not fetch object: %w", err))

	printObjectInfo(cmd, res.Object())
}

func printObjectInfo(cmd *cobra.Command, data []byte) {
	obj := object.New()
	err := obj.Unmarshal(data)
	common.ExitOnErr(cmd, common.Errf("can't unmarshal object: %w", err))

	if vHeader {
		cmd.Println("Version:", obj.Version())
		cmd.Println("Type:", obj.Type())
		cmd.Println("CID:", obj.ContainerID())
		cmd.Println("ID:", obj.ID())
		cmd.Println("Owner:", obj.OwnerID())
		cmd.Println("CreatedAt:", obj.CreationEpoch())
		cmd.Println("PayloadSize:", obj.PayloadSize())
		cmd.Println("Attributes:")
		for _, attr := range obj.Attributes() {
			cmd.Printf("  %s: %s\n", attr.Key(), attr.Value())
		}
	}

	if vOut != "" {
		err := ioutil.WriteFile(vOut, obj.Payload(), 0644)
		common.ExitOnErr(cmd, common.Errf("couldn't write payload: %w", err))
	}
}
