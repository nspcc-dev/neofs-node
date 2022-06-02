package container

import (
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/spf13/cobra"
)

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

		pk := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

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
	commonflags.Init(deleteContainerCmd)

	flags := deleteContainerCmd.Flags()

	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.BoolVar(&containerAwait, "await", false, "block execution until container is removed")
}
