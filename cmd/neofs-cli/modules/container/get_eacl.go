package container

import (
	"os"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
)

var getExtendedACLCmd = &cobra.Command{
	Use:   "get-eacl",
	Short: "Get extended ACL table of container",
	Long:  `Get extended ACL table of container`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		id := parseContainerID(cmd)
		cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

		var eaclPrm internalclient.EACLPrm
		eaclPrm.SetClient(cli)
		eaclPrm.SetContainer(id)

		res, err := internalclient.EACL(ctx, eaclPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		eaclTable := res.EACL()

		if containerPathTo == "" {
			cmd.Println("eACL: ")
			common.PrettyPrintJSON(cmd, &eaclTable, "eACL")

			return
		}

		var data []byte

		if containerJSON {
			data, err = eaclTable.MarshalJSON()
			common.ExitOnErr(cmd, "can't encode to JSON: %w", err)
		} else {
			data, err = eaclTable.Marshal()
			common.ExitOnErr(cmd, "can't encode to binary: %w", err)
		}

		cmd.Println("dumping data to file:", containerPathTo)

		err = os.WriteFile(containerPathTo, data, 0644)
		common.ExitOnErr(cmd, "could not write eACL to file: %w", err)
	},
}

func initContainerGetEACLCmd() {
	commonflags.Init(getExtendedACLCmd)

	flags := getExtendedACLCmd.Flags()

	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&containerPathTo, "to", "", "Path to dump encoded container (default: binary encoded)")
	flags.BoolVar(&containerJSON, commonflags.JSON, false, "Encode EACL table in json format")
}
