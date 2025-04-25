package container

import (
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

// flags of list command.
const (
	flagListPrintAttr      = "with-attr"
	flagListContainerOwner = "owner"
)

// flag vars of list command.
var (
	flagVarListPrintAttr      bool
	flagVarListContainerOwner string
)

var listContainersCmd = &cobra.Command{
	Use:   "list",
	Short: "List all created containers",
	Long:  "List all created containers",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		var idUser user.ID

		key, err := key.GetOrGenerate(cmd)
		if err != nil {
			return err
		}

		if flagVarListContainerOwner == "" {
			idUser = user.NewFromECDSAPublicKey(key.PublicKey)
		} else {
			err := idUser.DecodeString(flagVarListContainerOwner)
			if err != nil {
				return fmt.Errorf("invalid user ID: %w", err)
			}
		}

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		var prm internalclient.ListContainersPrm
		prm.SetClient(cli)
		prm.SetAccount(idUser)

		res, err := internalclient.ListContainers(ctx, prm)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		var prmGet internalclient.GetContainerPrm
		prmGet.SetClient(cli)

		list := res.IDList()
		for i := range list {
			cmd.Println(list[i].String())

			if flagVarListPrintAttr {
				prmGet.SetContainer(list[i])

				res, err := internalclient.GetContainer(ctx, prmGet)
				if err == nil {
					for key, val := range res.Container().UserAttributes() {
						cmd.Printf("  %s: %s\n", key, val)
					}
				} else {
					cmd.Printf("  failed to read attributes: %v\n", err)
				}
			}
		}
		return nil
	},
}

func initContainerListContainersCmd() {
	commonflags.Init(listContainersCmd)

	flags := listContainersCmd.Flags()

	flags.StringVar(&flagVarListContainerOwner, flagListContainerOwner, "",
		"Owner of containers (omit to use owner from private key)",
	)
	flags.BoolVar(&flagVarListPrintAttr, flagListPrintAttr, false,
		"Request and print attributes of each container",
	)
}
