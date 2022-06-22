package container

import (
	"bytes"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/spf13/cobra"
)

var eaclPathFrom string

var setExtendedACLCmd = &cobra.Command{
	Use:   "set-eacl",
	Short: "Set new extended ACL table for container",
	Long: `Set new extended ACL table for container.
Container ID in EACL table will be substituted with ID from the CLI.`,
	Run: func(cmd *cobra.Command, args []string) {
		id := parseContainerID(cmd)
		eaclTable := common.ReadEACL(cmd, eaclPathFrom)

		var tok *session.Container

		sessionTokenPath, _ := cmd.Flags().GetString(commonflags.SessionToken)
		if sessionTokenPath != "" {
			tok = new(session.Container)
			common.ReadSessionToken(cmd, tok, sessionTokenPath)
		}

		eaclTable.SetCID(id)

		pk := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

		var setEACLPrm internalclient.SetEACLPrm
		setEACLPrm.SetClient(cli)
		setEACLPrm.SetTable(*eaclTable)

		if tok != nil {
			setEACLPrm.WithinSession(*tok)
		}

		_, err := internalclient.SetEACL(setEACLPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		if containerAwait {
			exp, err := eaclTable.Marshal()
			common.ExitOnErr(cmd, "broken EACL table: %w", err)

			cmd.Println("awaiting...")

			var getEACLPrm internalclient.EACLPrm
			getEACLPrm.SetClient(cli)
			getEACLPrm.SetContainer(id)

			for i := 0; i < awaitTimeout; i++ {
				time.Sleep(1 * time.Second)

				res, err := internalclient.EACL(getEACLPrm)
				if err == nil {
					// compare binary values because EACL could have been set already
					got, err := res.EACL().Marshal()
					if err != nil {
						continue
					}

					if bytes.Equal(exp, got) {
						cmd.Println("EACL has been persisted on sidechain")
						return
					}
				}
			}

			common.ExitOnErr(cmd, "", errSetEACLTimeout)
		}
	},
}

func initContainerSetEACLCmd() {
	commonflags.Init(setExtendedACLCmd)

	flags := setExtendedACLCmd.Flags()
	flags.StringVar(&containerID, "cid", "", "container ID")
	flags.StringVar(&eaclPathFrom, "table", "", "path to file with JSON or binary encoded EACL table")
	flags.BoolVar(&containerAwait, "await", false, "block execution until EACL is persisted")
}
