package session

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/google/uuid"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	outFlag  = "out"
	jsonFlag = commonflags.JSON
)

const defaultLifetime = 10

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create session token",
	Long: `Create session token.

Default lifetime of session token is ` + strconv.Itoa(defaultLifetime) + ` epochs
if none of --` + commonflags.ExpireAt + ` or --` + commonflags.Lifetime + ` flags is specified.
`,
	Args: cobra.NoArgs,
	RunE: createSession,
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(commonflags.WalletPath, cmd.Flags().Lookup(commonflags.WalletPath))
		_ = viper.BindPFlag(commonflags.Account, cmd.Flags().Lookup(commonflags.Account))
	},
}

func init() {
	createCmd.Flags().Uint64P(commonflags.Lifetime, "l", defaultLifetime, "Number of epochs for token to stay valid")
	createCmd.Flags().StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	createCmd.Flags().StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	createCmd.Flags().String(outFlag, "", "File to write session token to")
	createCmd.Flags().Bool(jsonFlag, false, "Output token in JSON")
	createCmd.Flags().StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)
	createCmd.Flags().Uint64P(commonflags.ExpireAt, "e", 0, "The last active epoch for token to stay valid")

	_ = cobra.MarkFlagRequired(createCmd.Flags(), commonflags.WalletPath)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), outFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), commonflags.RPC)
	createCmd.MarkFlagsOneRequired(commonflags.ExpireAt, commonflags.Lifetime)
}

func createSession(cmd *cobra.Command, _ []string) error {
	privKey, err := key.Get(cmd)
	if err != nil {
		return err
	}

	var netAddr network.Address
	addrStr, _ := cmd.Flags().GetString(commonflags.RPC)
	if err := netAddr.FromString(addrStr); err != nil {
		return fmt.Errorf("can't parse endpoint: %w", err)
	}

	ctx := context.Background()
	c, err := internalclient.GetSDKClient(ctx, netAddr)
	if err != nil {
		return fmt.Errorf("can't create client: %w", err)
	}
	defer c.Close()

	endpoint, _ := cmd.Flags().GetString(commonflags.RPC)
	currEpoch, err := internalclient.GetCurrentEpoch(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("can't get current epoch: %w", err)
	}

	var exp uint64
	if exp, _ = cmd.Flags().GetUint64(commonflags.ExpireAt); exp == 0 {
		lifetime, _ := cmd.Flags().GetUint64(commonflags.Lifetime)
		exp = currEpoch + lifetime
	}
	if exp <= currEpoch {
		return errors.New("expiration epoch must be greater than current epoch")
	}
	var tok session.Object
	err = CreateSession(ctx, &tok, c, *privKey, exp, currEpoch)
	if err != nil {
		return fmt.Errorf("can't create session: %w", err)
	}

	var data []byte

	if toJSON, _ := cmd.Flags().GetBool(jsonFlag); toJSON {
		data, err = tok.MarshalJSON()
		if err != nil {
			return fmt.Errorf("can't decode session token JSON: %w", err)
		}
	} else {
		data = tok.Marshal()
	}

	filename, _ := cmd.Flags().GetString(outFlag)
	err = os.WriteFile(filename, data, 0o644)
	if err != nil {
		return fmt.Errorf("can't write token to file: %w", err)
	}

	return nil
}

// CreateSession opens a new communication with NeoFS storage node using client connection.
// The session is expected to be maintained by the storage node during the given
// number of epochs.
//
// Fills ID, lifetime and session key.
func CreateSession(ctx context.Context, dst *session.Object, c *client.Client, key ecdsa.PrivateKey, expireAt uint64, currEpoch uint64) error {
	var sessionPrm internalclient.CreateSessionPrm
	sessionPrm.SetClient(c)
	sessionPrm.SetExp(expireAt)
	sessionPrm.SetPrivateKey(key)

	sessionRes, err := internalclient.CreateSession(ctx, sessionPrm)
	if err != nil {
		return fmt.Errorf("can't open session: %w", err)
	}

	binIDSession := sessionRes.ID()

	var keySession neofsecdsa.PublicKey

	err = keySession.Decode(sessionRes.SessionKey())
	if err != nil {
		return fmt.Errorf("decode public session key: %w", err)
	}

	var idSession uuid.UUID

	err = idSession.UnmarshalBinary(binIDSession)
	if err != nil {
		return fmt.Errorf("decode session ID: %w", err)
	}

	dst.SetID(idSession)
	dst.SetNbf(currEpoch)
	dst.SetIat(currEpoch)
	dst.SetExp(expireAt)
	dst.SetAuthKey(&keySession)

	return nil
}
