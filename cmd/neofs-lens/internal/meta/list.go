package meta

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/spf13/cobra"
)

var listCMD = &cobra.Command{
	Use:   "list",
	Short: "List objects in metabase (metabase's List method)",
	Args:  cobra.NoArgs,
	Run:   listFunc,
}

var vLimit uint32

const limitFlagName = "limit"

func init() {
	listCMD.Flags().Uint32Var(&vLimit, limitFlagName, 0, "Number of objects to list")
	err := listCMD.MarkFlagRequired(limitFlagName)
	if err != nil {
		panic(fmt.Errorf("mark required flag %s failed: %w", limitFlagName, err))
	}

	common.AddComponentPathFlag(listCMD, &vPath)
}

func listFunc(cmd *cobra.Command, _ []string) {
	db := openMeta(cmd, true)
	defer db.Close()

	if vLimit == 0 {
		common.ExitOnErr(cmd, fmt.Errorf("%s flag must be positive", limitFlagName))
	}

	var prm meta.ListPrm
	prm.SetCount(vLimit)

	res, err := db.ListWithCursor(prm)
	common.ExitOnErr(cmd, common.Errf("metabase's `ListWithCursor`: %w", err))

	for _, addressWithType := range res.AddressList() {
		cmd.Printf("%s, Type: %s\n", addressWithType.Address, addressWithType.Type)
	}
}
