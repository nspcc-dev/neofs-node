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
	RunE:  signSessionToken,
}

func initSignSessionCmd() {
	commonflags.InitWithoutRPC(signSessionCmd)

	flags := signSessionCmd.Flags()

	flags.String(signFromFlag, "", "File with JSON encoded session token to sign")
	_ = signSessionCmd.MarkFlagFilename(signFromFlag)
	_ = signSessionCmd.MarkFlagRequired(signFromFlag)

	flags.String(signToFlag, "", "File to save signed session token (optional)")
}

func signSessionToken(cmd *cobra.Command, _ []string) error {
	fPath, err := cmd.Flags().GetString(signFromFlag)
	if err != nil {
		return err
	}

	if fPath == "" {
		return errors.New("missing session token flag")
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

	if errLast != nil {
		return fmt.Errorf("decode session: %v", errLast)
	}

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	err = stok.Sign(user.NewAutoIDSignerRFC6979(*pk))
	if err != nil {
		return fmt.Errorf("can't sign token: %w", err)
	}

	data, err := stok.MarshalJSON()
	if err != nil {
		return fmt.Errorf("can't encode session token: %w", err)
	}

	to := cmd.Flag(signToFlag).Value.String()
	if len(to) == 0 {
		common.PrettyPrintJSON(cmd, stok, "session token")
		return nil
	}

	err = os.WriteFile(to, data, 0o644)
	if err != nil {
		return fmt.Errorf("can't write signed session token to %s: %w", to, err)
	}

	cmd.Printf("signed session token saved in %s\n", to)

	return nil
}
