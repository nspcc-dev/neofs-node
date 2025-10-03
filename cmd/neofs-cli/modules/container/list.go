package container

import (
	"errors"
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/client"
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

		pk, err := key.Get(cmd)
		if err != nil && !errors.Is(err, key.ErrMissingFlag) {
			return err
		}

		if flagVarListContainerOwner != "" {
			err := idUser.DecodeString(flagVarListContainerOwner)
			if err != nil {
				return fmt.Errorf("invalid user ID: %w", err)
			}
		} else if pk != nil {
			idUser = user.NewFromECDSAPublicKey(pk.PublicKey)
		}

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		list, err := cli.ContainerList(ctx, idUser, client.PrmContainerList{})
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		for i := range list {
			cmd.Println(list[i].String())

			if flagVarListPrintAttr {
				cnr, err := cli.ContainerGet(ctx, list[i], client.PrmContainerGet{})
				if err == nil {
					for key, val := range cnr.UserAttributes() {
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
		"Owner of containers (omit to use owner from private key or if no key provided - list all containers)",
	)
	flags.BoolVar(&flagVarListPrintAttr, flagListPrintAttr, false,
		"Request and print attributes of each container",
	)
}
