package util

import (
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

const (
	signBearerJSONFlag = commonflags.JSON
)

var signBearerCmd = &cobra.Command{
	Use:   "bearer-token",
	Short: "Sign bearer token to use it in requests",
	Args:  cobra.NoArgs,
	RunE:  signBearerToken,
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

func signBearerToken(cmd *cobra.Command, _ []string) error {
	btok, err := common.ReadBearerToken(cmd, signFromFlag)
	if err != nil {
		return err
	}
	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	signer := user.NewAutoIDSignerRFC6979(*pk)
	var zeroUsr user.ID
	if issuer := btok.Issuer(); !issuer.Equals(zeroUsr) {
		// issuer is already set, don't corrupt it
		signer = user.NewSigner(signer, issuer)
	}

	err = btok.Sign(signer)
	if err != nil {
		return err
	}

	to := cmd.Flag(signToFlag).Value.String()
	jsonFlag, _ := cmd.Flags().GetBool(signBearerJSONFlag)

	var data []byte
	if jsonFlag || len(to) == 0 {
		data, err = btok.MarshalJSON()
		if err != nil {
			return fmt.Errorf("can't JSON encode bearer token: %w", err)
		}
	} else {
		data = btok.Marshal()
	}

	if len(to) == 0 {
		common.PrettyPrintJSON(cmd, btok, "bearer token")
		return nil
	}

	err = os.WriteFile(to, data, 0o644)
	if err != nil {
		return fmt.Errorf("can't write signed bearer token to file: %w", err)
	}

	cmd.Printf("signed bearer token was successfully dumped to %s\n", to)

	return nil
}
