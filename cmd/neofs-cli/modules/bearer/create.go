package bearer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

const (
	eaclFlag           = "eacl"
	issuedAtFlag       = "issued-at"
	notValidBeforeFlag = "not-valid-before"
	ownerFlag          = "owner"
	outFlag            = "out"
	jsonFlag           = commonflags.JSON
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create bearer token",
	Long: `Create bearer token.

All epoch flags can be specified relative to the current epoch with the +n syntax.
In this case --` + commonflags.RPC + ` flag should be specified and the epoch in bearer token
is set to current epoch + n.
`,
	Run: createToken,
}

func init() {
	createCmd.Flags().StringP(eaclFlag, "e", "", "path to the extended ACL table")
	createCmd.Flags().StringP(issuedAtFlag, "i", "", "epoch to issue token at")
	createCmd.Flags().StringP(notValidBeforeFlag, "n", "", "not valid before epoch")
	createCmd.Flags().StringP(commonflags.ExpireAt, "x", "", "expiration epoch")
	createCmd.Flags().StringP(ownerFlag, "o", "", "token owner")
	createCmd.Flags().String(outFlag, "", "file to write token to")
	createCmd.Flags().Bool(jsonFlag, false, "output token in JSON")
	createCmd.Flags().StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)

	_ = cobra.MarkFlagFilename(createCmd.Flags(), eaclFlag)

	_ = cobra.MarkFlagRequired(createCmd.Flags(), issuedAtFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), notValidBeforeFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), commonflags.ExpireAt)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), ownerFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), outFlag)
}

func createToken(cmd *cobra.Command, _ []string) {
	iat, iatRelative, err := common.ParseEpoch(cmd, issuedAtFlag)
	common.ExitOnErr(cmd, "can't parse --"+issuedAtFlag+" flag: %w", err)

	exp, expRelative, err := common.ParseEpoch(cmd, commonflags.ExpireAt)
	common.ExitOnErr(cmd, "can't parse --"+commonflags.ExpireAt+" flag: %w", err)

	nvb, nvbRelative, err := common.ParseEpoch(cmd, notValidBeforeFlag)
	common.ExitOnErr(cmd, "can't parse --"+notValidBeforeFlag+" flag: %w", err)

	if iatRelative || expRelative || nvbRelative {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		endpoint, _ := cmd.Flags().GetString(commonflags.RPC)
		currEpoch, err := internalclient.GetCurrentEpoch(ctx, endpoint)
		common.ExitOnErr(cmd, "can't fetch current epoch: %w", err)

		if iatRelative {
			iat += currEpoch
		}
		if expRelative {
			exp += currEpoch
		}
		if nvbRelative {
			nvb += currEpoch
		}
	}
	if exp < nvb {
		common.ExitOnErr(cmd, "",
			fmt.Errorf("expiration epoch is less than not-valid-before epoch: %d < %d", exp, nvb))
	}

	ownerStr, _ := cmd.Flags().GetString(ownerFlag)

	var ownerID user.ID
	common.ExitOnErr(cmd, "can't parse recipient: %w", ownerID.DecodeString(ownerStr))

	var b bearer.Token
	b.SetExp(exp)
	b.SetNbf(nvb)
	b.SetIat(iat)
	b.ForUser(ownerID)

	eaclPath, _ := cmd.Flags().GetString(eaclFlag)
	if eaclPath != "" {
		table := eaclSDK.NewTable()
		raw, err := os.ReadFile(eaclPath)
		common.ExitOnErr(cmd, "can't read extended ACL file: %w", err)
		common.ExitOnErr(cmd, "can't parse extended ACL: %w", json.Unmarshal(raw, table))
		b.SetEACLTable(*table)
	}

	var data []byte

	toJSON, _ := cmd.Flags().GetBool(jsonFlag)
	if toJSON {
		data, err = json.Marshal(b)
		common.ExitOnErr(cmd, "can't mashal token to JSON: %w", err)
	} else {
		data = b.Marshal()
	}

	out, _ := cmd.Flags().GetString(outFlag)
	err = os.WriteFile(out, data, 0644)
	common.ExitOnErr(cmd, "can't write token to file: %w", err)
}
