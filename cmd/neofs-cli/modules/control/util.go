package control

import (
	"context"
	"crypto/ecdsa"
	"errors"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
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

	var pubKey neofsecdsa.PublicKey
	common.ExitOnErr(cmd, "decode public key from signature: %w", pubKey.Decode(sigControl.GetKey()))

	sig := neofscrypto.NewSignature(neofscrypto.ECDSA_SHA512, &pubKey, sigControl.GetSign())

	if !sig.Verify(body.StableMarshal(nil)) {
		common.ExitOnErr(cmd, "", errors.New("invalid response signature"))
	}
}

func getClient(ctx context.Context, cmd *cobra.Command) *client.Client {
	return internalclient.GetSDKClientByFlag(ctx, cmd, controlRPC)
}
