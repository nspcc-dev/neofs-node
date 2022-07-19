package session

import (
	"fmt"
	"io/ioutil"

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
	RunE:  createSession,
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(commonflags.WalletPath, cmd.Flags().Lookup(commonflags.WalletPath))
		_ = viper.BindPFlag(commonflags.Account, cmd.Flags().Lookup(commonflags.Account))
	},
}

func init() {
	createCmd.Flags().Uint64P(commonflags.Lifetime, "l", defaultLifetime, "number of epochs for token to stay valid")
	createCmd.Flags().StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	createCmd.Flags().StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	createCmd.Flags().String(outFlag, "", "file to write session token to")
	createCmd.Flags().Bool(jsonFlag, false, "output token in JSON")
	createCmd.Flags().StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)

	_ = cobra.MarkFlagRequired(createCmd.Flags(), commonflags.Lifetime)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), commonflags.WalletPath)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), outFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), commonflags.RPC)
}

func createSession(cmd *cobra.Command, _ []string) error {
	privKey := key.Get(cmd)

	var netAddr network.Address
	addrStr, _ := cmd.Flags().GetString(commonflags.RPC)
	if err := netAddr.FromString(addrStr); err != nil {
		return err
	}

	c, err := internalclient.GetSDKClient(privKey, netAddr)
	if err != nil {
		return err
	}

	lifetime := uint64(defaultLifetime)
	if lfArg, _ := cmd.Flags().GetUint64(commonflags.Lifetime); lfArg != 0 {
		lifetime = lfArg
	}

	var tok session.Object

	err = CreateSession(&tok, c, lifetime)
	if err != nil {
		return err
	}

	var data []byte

	if toJSON, _ := cmd.Flags().GetBool(jsonFlag); toJSON {
		data, err = tok.MarshalJSON()
		if err != nil {
			return fmt.Errorf("decode session token JSON: %w", err)
		}
	} else {
		data = tok.Marshal()
	}

	filename, _ := cmd.Flags().GetString(outFlag)
	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("can't write token to file: %w", err)
	}
	return nil
}

// CreateSession opens a new communication with NeoFS storage node using client connection.
// The session is expected to be maintained by the storage node during the given
// number of epochs.
//
// Fills ID, lifetime and session key.
func CreateSession(dst *session.Object, c *client.Client, lifetime uint64) error {
	var netInfoPrm internalclient.NetworkInfoPrm
	netInfoPrm.SetClient(c)

	ni, err := internalclient.NetworkInfo(netInfoPrm)
	if err != nil {
		return fmt.Errorf("can't fetch network info: %w", err)
	}

	cur := ni.NetworkInfo().CurrentEpoch()
	exp := cur + lifetime

	var sessionPrm internalclient.CreateSessionPrm
	sessionPrm.SetClient(c)
	sessionPrm.SetExp(exp)

	sessionRes, err := internalclient.CreateSession(sessionPrm)
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
	dst.SetNbf(cur)
	dst.SetIat(cur)
	dst.SetExp(exp)
	dst.SetAuthKey(&keySession)

	return nil
}
