package control

import (
	"context"
	"crypto/ecdsa"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/spf13/cobra"
)

func initControlFlags(cmd *cobra.Command) {
	ff := cmd.Flags()
	ff.StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	ff.StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	ff.String(controlRPC, controlRPCDefault, controlRPCUsage)
	ff.DurationP(commonflags.Timeout, commonflags.TimeoutShorthand, commonflags.TimeoutDefault, commonflags.TimeoutUsage)
}

func signRequest(cmd *cobra.Command, pk *ecdsa.PrivateKey, req controlSvc.SignedMessage) {
	err := controlSvc.SignMessage(pk, req)
	common.ExitOnErr(cmd, "could not sign request: %w", err)
}

func verifyResponse(cmd *cobra.Command,
	sigControl interface {
		GetKey() []byte
		GetSign() []byte
	},
	body interface {
		StableMarshal([]byte) []byte
	},
) {
	if sigControl == nil {
		common.ExitOnErr(cmd, "", errors.New("missing response signature"))
	}

	// TODO(@cthulhu-rider): #1387 use Signature message from NeoFS API to avoid conversion
	var sigV2 refs.Signature
	sigV2.SetScheme(refs.ECDSA_SHA512)
	sigV2.SetKey(sigControl.GetKey())
	sigV2.SetSign(sigControl.GetSign())

	var sig neofscrypto.Signature
	common.ExitOnErr(cmd, "can't read signature: %w", sig.ReadFromV2(sigV2))

	if !sig.Verify(body.StableMarshal(nil)) {
		common.ExitOnErr(cmd, "", errors.New("invalid response signature"))
	}
}

func getClient(ctx context.Context, cmd *cobra.Command, pk *ecdsa.PrivateKey) *client.Client {
	return internalclient.GetSDKClientByFlag(ctx, cmd, pk, controlRPC)
}
