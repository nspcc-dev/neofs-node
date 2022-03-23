package token

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/token"
	"github.com/spf13/cobra"
)

const (
	eaclFlag           = "eacl"
	issuedAtFlag       = "issued-at"
	notValidBeforeFlag = "not-valid-before"
	expireAtFlag       = "expire-at"
	ownerFlag          = "owner"
	outFlag            = "out"
	jsonFlag           = "json"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create bearer token",
	RunE:  createToken,
}

func init() {
	createCmd.Flags().StringP(eaclFlag, "e", "", "path to the extended ACL table")
	createCmd.Flags().Uint64P(issuedAtFlag, "i", 0, "epoch to issue token at")
	createCmd.Flags().Uint64P(notValidBeforeFlag, "n", 0, "not valid before epoch")
	createCmd.Flags().Uint64P(expireAtFlag, "x", 0, "expiration epoch")
	createCmd.Flags().StringP(ownerFlag, "o", "", "token owner")
	createCmd.Flags().String(outFlag, "", "file to write token to")
	createCmd.Flags().Bool(jsonFlag, false, "output token in JSON")

	_ = cobra.MarkFlagFilename(createCmd.Flags(), eaclFlag)

	_ = cobra.MarkFlagRequired(createCmd.Flags(), issuedAtFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), notValidBeforeFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), expireAtFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), ownerFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), outFlag)
}

func createToken(cmd *cobra.Command, _ []string) error {
	iat, _ := cmd.Flags().GetUint64(issuedAtFlag)
	exp, _ := cmd.Flags().GetUint64(expireAtFlag)
	nvb, _ := cmd.Flags().GetUint64(notValidBeforeFlag)
	if exp < nvb {
		return fmt.Errorf("expiration epoch is less than not-valid-before epoch: %d < %d", exp, nvb)
	}

	ownerStr, _ := cmd.Flags().GetString(ownerFlag)
	ownerID := owner.NewID()
	if err := ownerID.Parse(ownerStr); err != nil {
		return fmt.Errorf("can't parse recipient: %w", err)
	}

	b := token.NewBearerToken()
	b.SetLifetime(exp, nvb, iat)
	b.SetOwner(ownerID)

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
		b.SetEACLTable(table)
	}

	var data []byte
	var err error

	toJSON, _ := cmd.Flags().GetBool(jsonFlag)
	if toJSON {
		data, err = json.Marshal(b)
	} else {
		data, err = b.Marshal(nil)
	}
	if err != nil {
		return fmt.Errorf("can't mashal token: %w", err)
	}

	out, _ := cmd.Flags().GetString(outFlag)
	if err := ioutil.WriteFile(out, data, 0644); err != nil {
		return fmt.Errorf("can't write token to file: %w", err)
	}

	return nil
}
