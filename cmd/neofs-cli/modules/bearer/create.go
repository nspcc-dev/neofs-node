package bearer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

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
	expireAtFlag       = "expire-at"
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
	RunE: createToken,
}

func init() {
	createCmd.Flags().StringP(eaclFlag, "e", "", "path to the extended ACL table")
	createCmd.Flags().StringP(issuedAtFlag, "i", "", "epoch to issue token at")
	createCmd.Flags().StringP(notValidBeforeFlag, "n", "", "not valid before epoch")
	createCmd.Flags().StringP(expireAtFlag, "x", "", "expiration epoch")
	createCmd.Flags().StringP(ownerFlag, "o", "", "token owner")
	createCmd.Flags().String(outFlag, "", "file to write token to")
	createCmd.Flags().Bool(jsonFlag, false, "output token in JSON")
	createCmd.Flags().StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)

	_ = cobra.MarkFlagFilename(createCmd.Flags(), eaclFlag)

	_ = cobra.MarkFlagRequired(createCmd.Flags(), issuedAtFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), notValidBeforeFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), expireAtFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), ownerFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), outFlag)
}

func createToken(cmd *cobra.Command, _ []string) error {
	iat, iatRelative, err := common.ParseEpoch(cmd, issuedAtFlag)
	if err != nil {
		return err
	}
	exp, expRelative, err := common.ParseEpoch(cmd, expireAtFlag)
	if err != nil {
		return err
	}
	nvb, nvbRelative, err := common.ParseEpoch(cmd, notValidBeforeFlag)
	if err != nil {
		return err
	}
	if iatRelative || expRelative || nvbRelative {
		endpoint, _ := cmd.Flags().GetString(commonflags.RPC)
		currEpoch, err := internalclient.GetCurrentEpoch(endpoint)
		if err != nil {
			return err
		}
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
		return fmt.Errorf("expiration epoch is less than not-valid-before epoch: %d < %d", exp, nvb)
	}

	ownerStr, _ := cmd.Flags().GetString(ownerFlag)

	var ownerID user.ID
	if err := ownerID.DecodeString(ownerStr); err != nil {
		return fmt.Errorf("can't parse recipient: %w", err)
	}

	var b bearer.Token
	b.SetExp(exp)
	b.SetNbf(nvb)
	b.SetIat(iat)
	b.ForUser(ownerID)

	eaclPath, _ := cmd.Flags().GetString(eaclFlag)
	if eaclPath != "" {
		table := eaclSDK.NewTable()
		raw, err := ioutil.ReadFile(eaclPath)
		if err != nil {
			return fmt.Errorf("can't read extended ACL file: %w", err)
		}
		if err := json.Unmarshal(raw, table); err != nil {
			return fmt.Errorf("can't parse extended ACL: %w", err)
		}
		b.SetEACLTable(*table)
	}

	var data []byte

	toJSON, _ := cmd.Flags().GetBool(jsonFlag)
	if toJSON {
		data, err = json.Marshal(b)
		if err != nil {
			return fmt.Errorf("can't mashal token to JSON: %w", err)
		}
	} else {
		data = b.Marshal()
	}

	out, _ := cmd.Flags().GetString(outFlag)
	if err := ioutil.WriteFile(out, data, 0644); err != nil {
		return fmt.Errorf("can't write token to file: %w", err)
	}

	return nil
}
