package session

import (
	"fmt"
	"io/ioutil"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/spf13/cobra"
)

const (
	lifetimeFlag = "lifetime"
	walletFlag   = "wallet"
	accountFlag  = "address"
	outFlag      = "out"
	jsonFlag     = "json"
	rpcFlag      = "rpc-endpoint"
)

const defaultLifetime = 10

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create session token",
	RunE:  createSession,
}

func init() {
	createCmd.Flags().Uint64P(lifetimeFlag, "l", defaultLifetime, "number of epochs for token to stay valid")
	createCmd.Flags().StringP(walletFlag, "w", "", "path to the wallet")
	createCmd.Flags().StringP(accountFlag, "a", "", "account address")
	createCmd.Flags().String(outFlag, "", "file to write session token to")
	createCmd.Flags().Bool(jsonFlag, false, "output token in JSON")
	createCmd.Flags().StringP(rpcFlag, "r", "", "rpc-endpoint")

	_ = cobra.MarkFlagRequired(createCmd.Flags(), lifetimeFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), walletFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), outFlag)
	_ = cobra.MarkFlagRequired(createCmd.Flags(), rpcFlag)
}

func createSession(cmd *cobra.Command, _ []string) error {
	walletPath, _ := cmd.Flags().GetString(walletFlag)
	accPath, _ := cmd.Flags().GetString(accountFlag)
	privKey, err := key.Get(walletPath, accPath)
	if err != nil {
		return err
	}

	var netAddr network.Address
	addrStr, _ := cmd.Flags().GetString(rpcFlag)
	if err := netAddr.FromString(addrStr); err != nil {
		return err
	}

	c, err := internalclient.GetSDKClient(privKey, netAddr)
	if err != nil {
		return err
	}

	lifetime := uint64(defaultLifetime)
	if lfArg, _ := cmd.Flags().GetUint64(lifetimeFlag); lfArg != 0 {
		lifetime = lfArg
	}

	ownerID := owner.NewIDFromPublicKey(&privKey.PublicKey)
	tok, err := CreateSession(c, ownerID, lifetime)
	if err != nil {
		return err
	}

	var data []byte

	if toJSON, _ := cmd.Flags().GetBool(jsonFlag); toJSON {
		data, err = tok.MarshalJSON()
	} else {
		data = tok.Marshal()
	}
	if err != nil {
		return fmt.Errorf("can't marshal token: %w", err)
	}

	filename, _ := cmd.Flags().GetString(outFlag)
	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("can't write token to file: %w", err)
	}
	return nil
}

// CreateSession returns newly created session token with the specified owner and lifetime.
// `Issued-At` and `Not-Valid-Before` fields are set to current epoch.
func CreateSession(c *client.Client, owner *owner.ID, lifetime uint64) (*session.Token, error) {
	var netInfoPrm internalclient.NetworkInfoPrm
	netInfoPrm.SetClient(c)

	ni, err := internalclient.NetworkInfo(netInfoPrm)
	if err != nil {
		return nil, fmt.Errorf("can't fetch network info: %w", err)
	}

	cur := ni.NetworkInfo().CurrentEpoch()
	exp := cur + lifetime

	var sessionPrm internalclient.CreateSessionPrm
	sessionPrm.SetClient(c)
	sessionPrm.SetExp(exp)

	sessionRes, err := internalclient.CreateSession(sessionPrm)
	if err != nil {
		return nil, fmt.Errorf("can't open session: %w", err)
	}

	tok := session.NewToken()
	tok.SetID(sessionRes.ID())
	tok.SetSessionKey(sessionRes.SessionKey())
	tok.SetOwnerID(owner)
	tok.SetExp(exp)
	tok.SetIat(cur)
	tok.SetNbf(cur)

	return tok, nil
}
