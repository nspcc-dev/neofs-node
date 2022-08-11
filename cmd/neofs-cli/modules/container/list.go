package container

import (
	"strings"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

// flags of list command
const (
	flagListPrintAttr      = "with-attr"
	flagListContainerOwner = "owner"
)

// flag vars of list command
var (
	flagVarListPrintAttr      bool
	flagVarListContainerOwner string
)

var listContainersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all created containers",
	Long:  "List all created containers",
	Run: func(cmd *cobra.Command, args []string) {
		var idUser user.ID

		key := key.GetOrGenerate(cmd)

		if flagVarListContainerOwner == "" {
			user.IDFromKey(&idUser, key.PublicKey)
		} else {
			err := idUser.DecodeString(flagVarListContainerOwner)
			common.ExitOnErr(cmd, "invalid user ID: %w", err)
		}

		cli := internalclient.GetSDKClientByFlag(cmd, key, commonflags.RPC)

		var prm internalclient.ListContainersPrm
		prm.SetClient(cli)
		prm.SetAccount(idUser)

		res, err := internalclient.ListContainers(prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		var prmGet internalclient.GetContainerPrm
		prmGet.SetClient(cli)

		list := res.IDList()
		for i := range list {
			cmd.Println(list[i].String())

			if flagVarListPrintAttr {
				prmGet.SetContainer(list[i])

				res, err := internalclient.GetContainer(prmGet)
				if err == nil {
					res.Container().IterateAttributes(func(key, val string) {
						if !strings.HasPrefix(key, container.SysAttributePrefix) {
							// FIXME(@cthulhu-rider): neofs-sdk-go#314 use dedicated method to skip system attributes
							cmd.Printf("  %s: %s\n", key, val)
						}
					})
				} else {
					cmd.Printf("  failed to read attributes: %v\n", err)
				}
			}
		}
	},
}

func initContainerListContainersCmd() {
	commonflags.Init(listContainersCmd)

	flags := listContainersCmd.Flags()

	flags.StringVar(&flagVarListContainerOwner, flagListContainerOwner, "",
		"owner of containers (omit to use owner from private key)",
	)
	flags.BoolVar(&flagVarListPrintAttr, flagListPrintAttr, false,
		"request and print attributes of each container",
	)
}
