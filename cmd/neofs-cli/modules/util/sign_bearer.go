package util

import (
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/spf13/cobra"
)

const (
	signBearerJSONFlag = commonflags.JSON
)

var signBearerCmd = &cobra.Command{
	Use:   "bearer-token",
	Short: "Sign bearer token to use it in requests",
	Run:   signBearerToken,
}

func initSignBearerCmd() {
	commonflags.InitWithoutRPC(signBearerCmd)

	flags := signBearerCmd.Flags()

	flags.String(signFromFlag, "", "File with JSON or binary encoded bearer token to sign")
	_ = signBearerCmd.MarkFlagFilename(signFromFlag)
	_ = signBearerCmd.MarkFlagRequired(signFromFlag)

	flags.String(signToFlag, "", "File to dump signed bearer token (default: binary encoded)")
	flags.Bool(signBearerJSONFlag, false, "Dump bearer token in JSON encoding")
}

func signBearerToken(cmd *cobra.Command, _ []string) {
	btok := common.ReadBearerToken(cmd, signFromFlag)
	pk := key.GetOrGenerate(cmd)

	err := btok.Sign(*pk)
	common.ExitOnErr(cmd, "", err)

	to := cmd.Flag(signToFlag).Value.String()
	jsonFlag, _ := cmd.Flags().GetBool(signBearerJSONFlag)

	var data []byte
	if jsonFlag || len(to) == 0 {
		data, err = btok.MarshalJSON()
		common.ExitOnErr(cmd, "can't JSON encode bearer token: %w", err)
	} else {
		data = btok.Marshal()
	}

	if len(to) == 0 {
		prettyPrintJSON(cmd, data)

		return
	}

	err = os.WriteFile(to, data, 0644)
	common.ExitOnErr(cmd, "can't write signed bearer token to file: %w", err)

	cmd.Printf("signed bearer token was successfully dumped to %s\n", to)
}
