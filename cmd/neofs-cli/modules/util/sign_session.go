package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

var signSessionCmd = &cobra.Command{
	Use:   "session-token",
	Short: "Sign session token to use it in requests",
	Args:  cobra.NoArgs,
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
	fPath, err := cmd.Flags().GetString(signFromFlag)
	common.ExitOnErr(cmd, "", err)

	if fPath == "" {
		common.ExitOnErr(cmd, "", errors.New("missing session token flag"))
	}

	type iTokenSession interface {
		json.Marshaler
		common.BinaryOrJSON
		Sign(user.Signer) error
	}
	var errLast error
	var stok iTokenSession

	for _, el := range [...]iTokenSession{
		new(session.Object),
		new(session.Container),
	} {
		errLast = common.ReadBinaryOrJSON(cmd, el, fPath)
		if errLast == nil {
			stok = el
			break
		}
	}

	common.ExitOnErr(cmd, "decode session: %v", errLast)

	pk := key.GetOrGenerate(cmd)

	err = stok.Sign(user.NewAutoIDSignerRFC6979(*pk))
	common.ExitOnErr(cmd, "can't sign token: %w", err)

	data, err := stok.MarshalJSON()
	common.ExitOnErr(cmd, "can't encode session token: %w", err)

	to := cmd.Flag(signToFlag).Value.String()
	if len(to) == 0 {
		common.PrettyPrintJSON(cmd, stok, "session token")
		return
	}

	err = os.WriteFile(to, data, 0o644)
	if err != nil {
		common.ExitOnErr(cmd, "", fmt.Errorf("can't write signed session token to %s: %w", to, err))
	}

	cmd.Printf("signed session token saved in %s\n", to)
}
