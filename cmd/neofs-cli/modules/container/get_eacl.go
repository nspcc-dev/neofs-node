package container

import (
	"os"

	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/spf13/cobra"
)

var getExtendedACLCmd = &cobra.Command{
	Use:   "get-eacl",
	Short: "Get extended ACL table of container",
	Long:  `Get extended ACL talbe of container`,
	Run: func(cmd *cobra.Command, args []string) {
		id := parseContainerID(cmd)
		pk := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

		var eaclPrm internalclient.EACLPrm
		eaclPrm.SetClient(cli)
		eaclPrm.SetContainer(id)

		res, err := internalclient.EACL(eaclPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		eaclTable := res.EACL()

		sig := eaclTable.Signature()

		// TODO(@cthulhu-rider): #1387 avoid type conversion
		var sigV2 refs.Signature
		sig.WriteToV2(&sigV2)

		if containerPathTo == "" {
			cmd.Println("eACL: ")
			common.PrettyPrintJSON(cmd, eaclTable, "eACL")

			var sigV2 refs.Signature
			sig.WriteToV2(&sigV2)

			cmd.Println("Signature:")
			common.PrettyPrintJSON(cmd, &sigV2, "signature")

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

		cmd.Println("Signature:")
		common.PrettyPrintJSON(cmd, &sigV2, "signature")

		err = os.WriteFile(containerPathTo, data, 0644)
		common.ExitOnErr(cmd, "could not write eACL to file: %w", err)
	},
}

func initContainerGetEACLCmd() {
	commonflags.Init(getExtendedACLCmd)

	flags := getExtendedACLCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.StringVar(&containerPathTo, "to", "", "path to dump encoded container (default: binary encoded)")
	flags.BoolVar(&containerJSON, "json", false, "encode EACL table in json format")
}
