package container

import (
	"fmt"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

var deleteContainerCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete existing container",
	Long: `Delete existing container. 
Only owner of the container has a permission to remove container.`,
	Run: func(cmd *cobra.Command, args []string) {
		id := parseContainerID(cmd)

		tok := getSession(cmd)

		pk := key.Get(cmd)
		cli := internalclient.GetSDKClientByFlag(cmd, pk, commonflags.RPC)

		if force, _ := cmd.Flags().GetBool(commonflags.ForceFlag); !force {
			common.PrintVerbose(cmd, "Reading the container to check ownership...")

			var getPrm internalclient.GetContainerPrm
			getPrm.SetClient(cli)
			getPrm.SetContainer(id)

			resGet, err := internalclient.GetContainer(getPrm)
			common.ExitOnErr(cmd, "can't get the container: %w", err)

			owner := resGet.Container().Owner()

			if tok != nil {
				common.PrintVerbose(cmd, "Checking session issuer...")

				if !tok.Issuer().Equals(owner) {
					common.ExitOnErr(cmd, "", fmt.Errorf("session issuer differs with the container owner: expected %s, has %s", owner, tok.Issuer()))
				}
			} else {
				common.PrintVerbose(cmd, "Checking provided account...")

				var acc user.ID
				err = user.IDFromSigner(&acc, neofsecdsa.SignerRFC6979(*pk))
				common.ExitOnErr(cmd, "decoding user from key", err)

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
				searchPrm.SetContainerID(id)
				searchPrm.SetFilters(fs)
				searchPrm.SetTTL(2)

				common.PrintVerbose(cmd, "Searching for LOCK objects...")

				res, err := internalclient.SearchObjects(searchPrm)
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

	flags.StringVar(&containerID, commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.BoolVar(&containerAwait, "await", false, "Block execution until container is removed")
	flags.BoolP(commonflags.ForceFlag, commonflags.ForceFlagShorthand, false, "Skip validation checks (ownership, presence of LOCK objects)")
}
