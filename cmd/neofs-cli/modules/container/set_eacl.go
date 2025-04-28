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
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/util"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

var flagVarsSetEACL struct {
	srcPath string
}

var setExtendedACLCmd = &cobra.Command{
	Use:   "set-eacl",
	Short: "Set new extended ACL table for container",
	Long: `Set new extended ACL table for container.
Container ID in EACL table will be substituted with ID from the CLI.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		id, err := parseContainerID()
		if err != nil {
			return err
		}
		eaclTable, err := common.ReadEACL(cmd, flagVarsSetEACL.srcPath)
		if err != nil {
			return err
		}

		tok, err := getSession(cmd)
		if err != nil {
			return err
		}

		eaclTable.SetCID(id)

		pk, err := key.GetOrGenerate(cmd)
		if err != nil {
			return err
		}

		ctx, cancel := getAwaitContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()
		force, _ := cmd.Flags().GetBool(commonflags.ForceFlag)
		if !force {
			common.PrintVerbose(cmd, "Validating eACL table...")
			err := util.ValidateEACLTable(eaclTable)
			if err != nil {
				return fmt.Errorf("table validation: %w", err)
			}

			cmd.Println("Checking the ability to modify access rights in the container...")
			common.PrintVerbose(cmd, "Reading the container to check ownership...")

			var getPrm internalclient.GetContainerPrm
			getPrm.SetClient(cli)
			getPrm.SetContainer(id)

			resGet, err := internalclient.GetContainer(ctx, getPrm)
			if err != nil {
				return fmt.Errorf("can't get the container: %w", err)
			}

			cnr := resGet.Container()
			owner := cnr.Owner()

			if tok != nil {
				common.PrintVerbose(cmd, "Checking session issuer...")

				if tok.Issuer() != owner {
					return fmt.Errorf("session issuer differs with the container owner: expected %s, has %s", owner, tok.Issuer())
				}
			} else {
				common.PrintVerbose(cmd, "Checking provided account...")

				acc := user.NewFromECDSAPublicKey(pk.PublicKey)

				if acc != owner {
					return fmt.Errorf("provided account differs with the container owner: expected %s, has %s", owner, acc)
				}
			}

			common.PrintVerbose(cmd, "Account matches the container owner.")

			extendable := cnr.BasicACL().Extendable()

			if !extendable {
				return errors.New("container ACL is immutable")
			}

			cmd.Println("ACL extension is enabled in the container, continue processing.")
		}

		var setEACLPrm internalclient.SetEACLPrm
		setEACLPrm.SetClient(cli)
		setEACLPrm.SetTable(eaclTable)
		setEACLPrm.SetPrivateKey(*pk)

		if tok != nil {
			setEACLPrm.WithinSession(*tok)
		}

		_, err = internalclient.SetEACL(ctx, setEACLPrm)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		cmd.Println("eACL modification request accepted for processing (the operation may not be completed yet)")

		if containerAwait {
			exp := eaclTable.Marshal()

			cmd.Println("awaiting...")

			var getEACLPrm internalclient.EACLPrm
			getEACLPrm.SetClient(cli)
			getEACLPrm.SetContainer(id)

			const waitInterval = time.Second

			t := time.NewTimer(waitInterval)

			for ; ; t.Reset(waitInterval) {
				select {
				case <-ctx.Done():
					return fmt.Errorf("eACL setting: %w", common.ErrAwaitTimeout)
				case <-t.C:
				}

				res, err := internalclient.EACL(ctx, getEACLPrm)
				if err == nil {
					// compare binary values because EACL could have been set already
					table := res.EACL()
					got := table.Marshal()

					if bytes.Equal(exp, got) {
						cmd.Println("EACL has been persisted on FS chain")
						return nil
					}
				}
			}

		}
		return nil
	},
}

func initContainerSetEACLCmd() {
	commonflags.Init(setExtendedACLCmd)

	flags := setExtendedACLCmd.Flags()
	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.StringVar(&flagVarsSetEACL.srcPath, "table", "", "path to file with JSON or binary encoded EACL table")
	flags.BoolVar(&containerAwait, "await", false, fmt.Sprintf("block execution until EACL is persisted. "+
		"Increases default execution timeout to %.0fs", awaitTimeout.Seconds())) // simple %s notation prints 1m0s https://github.com/golang/go/issues/39064
	flags.BoolP(commonflags.ForceFlag, commonflags.ForceFlagShorthand, false, "skip validation checks (ownership, extensibility of the container ACL)")
}
