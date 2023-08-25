package container

import (
	"fmt"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

var deleteContainerCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete existing container",
	Long: `Delete existing container. 
Only owner of the container has a permission to remove container.`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		ctx, cancel := getAwaitContext(cmd)
		defer cancel()

		id := parseContainerID(cmd)

		tok := getSession(cmd)

		pk := key.Get(cmd)
		cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

		if force, _ := cmd.Flags().GetBool(commonflags.ForceFlag); !force {
			common.PrintVerbose(cmd, "Reading the container to check ownership...")

			var getPrm internalclient.GetContainerPrm
			getPrm.SetClient(cli)
			getPrm.SetContainer(id)

			resGet, err := internalclient.GetContainer(ctx, getPrm)
			common.ExitOnErr(cmd, "can't get the container: %w", err)

			owner := resGet.Container().Owner()

			if tok != nil {
				common.PrintVerbose(cmd, "Checking session issuer...")

				if !tok.Issuer().Equals(owner) {
					common.ExitOnErr(cmd, "", fmt.Errorf("session issuer differs with the container owner: expected %s, has %s", owner, tok.Issuer()))
				}
			} else {
				common.PrintVerbose(cmd, "Checking provided account...")

				acc := user.ResolveFromECDSAPublicKey(pk.PublicKey)

				if !acc.Equals(owner) {
					common.ExitOnErr(cmd, "", fmt.Errorf("provided account differs with the container owner: expected %s, has %s", owner, acc))
				}
			}

			common.PrintVerbose(cmd, "Account matches the container owner.")

			if tok != nil {
				common.PrintVerbose(cmd, "Skip searching for LOCK objects - session provided.")
			} else {
				fs := objectSDK.NewSearchFilters()
				fs.AddTypeFilter(objectSDK.MatchStringEqual, objectSDK.TypeLock)

				var searchPrm internalclient.SearchObjectsPrm
				searchPrm.SetClient(cli)
				searchPrm.SetPrivateKey(*pk)
				searchPrm.SetContainerID(id)
				searchPrm.SetFilters(fs)
				searchPrm.SetTTL(2)

				common.PrintVerbose(cmd, "Searching for LOCK objects...")

				res, err := internalclient.SearchObjects(ctx, searchPrm)
				common.ExitOnErr(cmd, "can't search for LOCK objects: %w", err)

				if len(res.IDList()) != 0 {
					common.ExitOnErr(cmd, "",
						fmt.Errorf("Container wasn't removed because LOCK objects were found.\n"+
							"Use --%s flag to remove anyway.", commonflags.ForceFlag))
				}
			}
		}

		var delPrm internalclient.DeleteContainerPrm
		delPrm.SetClient(cli)
		delPrm.SetContainer(id)
		delPrm.SetPrivateKey(*pk)

		if tok != nil {
			delPrm.WithinSession(*tok)
		}

		_, err := internalclient.DeleteContainer(ctx, delPrm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		cmd.Println("container removal request accepted for processing (the operation may not be completed yet)")

		if containerAwait {
			cmd.Println("awaiting...")

			var getPrm internalclient.GetContainerPrm
			getPrm.SetClient(cli)
			getPrm.SetContainer(id)

			const waitInterval = time.Second

			t := time.NewTimer(waitInterval)
			defer t.Stop()

			for ; ; t.Reset(waitInterval) {
				select {
				case <-ctx.Done():
					common.ExitOnErr(cmd, "container deletion: %s", common.ErrAwaitTimeout)
				case <-t.C:
				}

				_, err := internalclient.GetContainer(ctx, getPrm)
				if err != nil {
					cmd.Println("container has been removed:", containerID)
					return
				}
			}
		}
	},
}

func initContainerDeleteCmd() {
	flags := deleteContainerCmd.Flags()

	flags.StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	flags.StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	flags.StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)

	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.BoolVar(&containerAwait, "await", false, fmt.Sprintf("Block execution until container is removed. "+
		"Increases default execution timeout to %.0fs", awaitTimeout.Seconds())) // simple %s notation prints 1m0s https://github.com/golang/go/issues/39064
	flags.BoolP(commonflags.ForceFlag, commonflags.ForceFlagShorthand, false, "Skip validation checks (ownership, presence of LOCK objects)")
}
