package container

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/spf13/cobra"
)

var flagVarsSetEACL struct {
	noPreCheck bool

	srcPath string
}

var setExtendedACLCmd = &cobra.Command{
	Use:   "set-eacl",
	Short: "Set new extended ACL table for container",
	Long: `Set new extended ACL table for container.
Container ID in EACL table will be substituted with ID from the CLI.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := getAwaitContext(cmd)
		defer cancel()

		id := parseContainerID(cmd)
		eaclTable := common.ReadEACL(cmd, flagVarsSetEACL.srcPath)

		tok := getSession(cmd)

		eaclTable.SetCID(id)

		pk := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

		if !flagVarsSetEACL.noPreCheck {
			cmd.Println("Checking the ability to modify access rights in the container...")

			extendable, err := internalclient.IsACLExtendable(ctx, cli, id)
			common.ExitOnErr(cmd, "Extensibility check failure: %w", err)

			if !extendable {
				common.ExitOnErr(cmd, "", errors.New("container ACL is immutable"))
			}

			cmd.Println("ACL extension is enabled in the container, continue processing.")
		}

		var setEACLPrm internalclient.SetEACLPrm
		setEACLPrm.SetClient(cli)
		setEACLPrm.SetTable(*eaclTable)
		setEACLPrm.SetPrivateKey(*pk)

		if tok != nil {
			setEACLPrm.WithinSession(*tok)
		}

		_, err := internalclient.SetEACL(ctx, setEACLPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		cmd.Println("eACL modification request accepted for processing (the operation may not be completed yet)")

		if containerAwait {
			exp, err := eaclTable.Marshal()
			common.ExitOnErr(cmd, "broken EACL table: %w", err)

			cmd.Println("awaiting...")

			var getEACLPrm internalclient.EACLPrm
			getEACLPrm.SetClient(cli)
			getEACLPrm.SetContainer(id)

			const waitInterval = time.Second

			t := time.NewTimer(waitInterval)
			defer t.Stop()

			for ; ; t.Reset(waitInterval) {
				select {
				case <-ctx.Done():
					common.ExitOnErr(cmd, "", errSetEACLTimeout)
				case <-t.C:
				}

				res, err := internalclient.EACL(ctx, getEACLPrm)
				if err == nil {
					// compare binary values because EACL could have been set already
					table := res.EACL()
					got, err := table.Marshal()
					if err != nil {
						continue
					}

					if bytes.Equal(exp, got) {
						cmd.Println("EACL has been persisted on sidechain")
						return
					}
				}
			}

		}
	},
}

func initContainerSetEACLCmd() {
	commonflags.Init(setExtendedACLCmd)

	flags := setExtendedACLCmd.Flags()
	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&flagVarsSetEACL.srcPath, "table", "", "path to file with JSON or binary encoded EACL table")
	flags.BoolVar(&containerAwait, "await", false, fmt.Sprintf("block execution until EACL is persisted. "+
		"Increases default execution timeout to %.0fs", awaitTimeout.Seconds())) // simple %s notation prints 1m0s https://github.com/golang/go/issues/39064
	flags.BoolVar(&flagVarsSetEACL.noPreCheck, "no-precheck", false, "do not pre-check the extensibility of the container ACL")
}
