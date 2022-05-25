package util

import (
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/spf13/cobra"
)

var signSessionCmd = &cobra.Command{
	Use:   "session-token",
	Short: "Sign session token to use it in requests",
	Run:   signSessionToken,
}

func initSignSessionCmd() {
	commonflags.InitWithoutRPC(signSessionCmd)

	flags := signSessionCmd.Flags()

	flags.String(signFromFlag, "", "File with JSON encoded session token to sign")
	_ = signSessionCmd.MarkFlagFilename(signFromFlag)
	_ = signSessionCmd.MarkFlagRequired(signFromFlag)

	flags.String(signToFlag, "", "File to save signed session token (optional)")
}

func signSessionToken(cmd *cobra.Command, _ []string) {
	stok := common.ReadSessionToken(cmd, signFromFlag)
	pk := key.GetOrGenerate(cmd)

	err := stok.Sign(pk)
	common.ExitOnErr(cmd, "can't sign token: %w", err)

	data, err := stok.MarshalJSON()
	common.ExitOnErr(cmd, "can't encode session token: %w", err)

	to := cmd.Flag(signToFlag).Value.String()
	if len(to) == 0 {
		prettyPrintJSON(cmd, data)
		return
	}

	err = os.WriteFile(to, data, 0644)
	if err != nil {
		common.ExitOnErr(cmd, "", fmt.Errorf("can't write signed session token to %s: %w", to, err))
	}

	cmd.Printf("signed session token saved in %s\n", to)
}
