package container

import (
	"fmt"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/spf13/cobra"
)

const forceFlag = "force"

var deleteContainerCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete existing container",
	Long: `Delete existing container. 
Only owner of the container has a permission to remove container.`,
	Run: func(cmd *cobra.Command, args []string) {
		id := parseContainerID(cmd)

		var tok *session.Container

		sessionTokenPath, _ := cmd.Flags().GetString(commonflags.SessionToken)
		if sessionTokenPath != "" {
			tok = new(session.Container)
			common.ReadSessionToken(cmd, tok, sessionTokenPath)
		}

		pk := key.Get(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

		if force, _ := cmd.Flags().GetBool(forceFlag); !force {
			fs := objectSDK.NewSearchFilters()
			fs.AddTypeFilter(objectSDK.MatchStringEqual, objectSDK.TypeLock)

			var searchPrm internalclient.SearchObjectsPrm
			searchPrm.SetClient(cli)
			searchPrm.SetContainerID(id)
			searchPrm.SetFilters(fs)
			searchPrm.SetTTL(2)

			common.PrintVerbose("Searching for LOCK objects...")

			res, err := internalclient.SearchObjects(searchPrm)
			common.ExitOnErr(cmd, "can't search for LOCK objects: %w", err)

			if len(res.IDList()) != 0 {
				common.ExitOnErr(cmd, "",
					fmt.Errorf("Container wasn't removed because LOCK objects were found.\n"+
						"Use --%s flag to remove anyway.", forceFlag))
			}
		}

		var delPrm internalclient.DeleteContainerPrm
		delPrm.SetClient(cli)
		delPrm.SetContainer(id)

		if tok != nil {
			delPrm.WithinSession(*tok)
		}

		_, err := internalclient.DeleteContainer(delPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		cmd.Println("container delete method invoked")

		if containerAwait {
			cmd.Println("awaiting...")

			var getPrm internalclient.GetContainerPrm
			getPrm.SetClient(cli)
			getPrm.SetContainer(id)

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				_, err := internalclient.GetContainer(getPrm)
				if err != nil {
					cmd.Println("container has been removed:", containerID)
					return
				}
			}

			common.ExitOnErr(cmd, "", errDeleteTimeout)
		}
	},
}

func initContainerDeleteCmd() {
	flags := deleteContainerCmd.Flags()

	flags.StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	flags.StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	flags.StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.BoolVar(&containerAwait, "await", false, "block execution until container is removed")
	flags.Bool(forceFlag, false, "do not check whether container contains locks and remove immediately")
}
