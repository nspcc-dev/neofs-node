package bearer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
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
	jsonFlag           = "json"
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
	iat, iatRelative, err := parseEpoch(cmd, issuedAtFlag)
	if err != nil {
		return err
	}
	exp, expRelative, err := parseEpoch(cmd, expireAtFlag)
	if err != nil {
		return err
	}
	nvb, nvbRelative, err := parseEpoch(cmd, notValidBeforeFlag)
	if err != nil {
		return err
	}
	if iatRelative || expRelative || nvbRelative {
		endpoint, _ := cmd.Flags().GetString(commonflags.RPC)
		currEpoch, err := getCurrentEpoch(endpoint)
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
	b.SetExpiration(exp)
	b.SetNotBefore(nvb)
	b.SetIssuedAt(iat)
	b.SetOwnerID(ownerID)

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

// parseEpoch parses epoch argument. Second return value is true if
// the specified epoch is relative, and false otherwise.
func parseEpoch(cmd *cobra.Command, flag string) (uint64, bool, error) {
	s, _ := cmd.Flags().GetString(flag)
	if len(s) == 0 {
		return 0, false, nil
	}

	relative := s[0] == '+'
	if relative {
		s = s[1:]
	}

	epoch, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, relative, fmt.Errorf("can't parse epoch for %s argument: %w", flag, err)
	}
	return epoch, relative, nil
}

// getCurrentEpoch returns current epoch.
func getCurrentEpoch(endpoint string) (uint64, error) {
	var addr network.Address

	if err := addr.FromString(endpoint); err != nil {
		return 0, fmt.Errorf("can't parse RPC endpoint: %w", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return 0, fmt.Errorf("can't generate key to sign query: %w", err)
	}

	c, err := internalclient.GetSDKClient(key, addr)
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	ni, err := c.NetworkInfo(ctx, client.PrmNetworkInfo{})
	if err != nil {
		return 0, err
	}

	return ni.Info().CurrentEpoch(), nil
}
