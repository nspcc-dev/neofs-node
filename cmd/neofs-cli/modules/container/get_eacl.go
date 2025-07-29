package container

import (
	"fmt"
	"os"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/spf13/cobra"
)

var getExtendedACLCmd = &cobra.Command{
	Use:   "get-eacl",
	Short: "Get extended ACL table of container",
	Long:  `Get extended ACL table of container`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		id, err := parseContainerID()
		if err != nil {
			return err
		}
		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		eaclTable, err := cli.ContainerEACL(ctx, id, client.PrmContainerEACL{})
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		if containerPathTo == "" {
			cmd.Println("eACL: ")
			common.PrettyPrintJSON(cmd, &eaclTable, "eACL")

			return nil
		}

		var data []byte

		if containerJSON {
			data, err = eaclTable.MarshalJSON()
			if err != nil {
				return fmt.Errorf("can't encode to JSON: %w", err)
			}
		} else {
			data = eaclTable.Marshal()
		}

		cmd.Println("dumping data to file:", containerPathTo)

		err = os.WriteFile(containerPathTo, data, 0644)
		if err != nil {
			return fmt.Errorf("could not write eACL to file: %w", err)
		}
		return nil
	},
}

func initContainerGetEACLCmd() {
	commonflags.Init(getExtendedACLCmd)

	flags := getExtendedACLCmd.Flags()

	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&containerPathTo, "to", "", "Path to dump encoded container (default: binary encoded)")
	flags.BoolVar(&containerJSON, commonflags.JSON, false, "Encode EACL table in json format")
}
