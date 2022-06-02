package container

import (
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

var containerOwner string

var listContainersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all created containers",
	Long:  "List all created containers",
	Run: func(cmd *cobra.Command, args []string) {
		var idUser user.ID

		key := key.GetOrGenerate(cmd)

		if containerOwner == "" {
			user.IDFromKey(&idUser, key.PublicKey)
		} else {
			err := idUser.DecodeString(containerOwner)
			common.ExitOnErr(cmd, "invalid user ID: %w", err)
		}

		cli := internalclient.GetSDKClientByFlag(cmd, key, commonflags.RPC)

		var prm internalclient.ListContainersPrm
		prm.SetClient(cli)
		prm.SetAccount(idUser)

		res, err := internalclient.ListContainers(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		// print to stdout
		prettyPrintContainerList(cmd, res.IDList())
	},
}

func initContainerListContainersCmd() {
	commonflags.Init(listContainersCmd)

	flags := listContainersCmd.Flags()

	flags.StringVar(&containerOwner, "owner", "", "owner of containers (omit to use owner from private key)")
}

func prettyPrintContainerList(cmd *cobra.Command, list []cid.ID) {
	for i := range list {
		cmd.Println(list[i].String())
	}
}
