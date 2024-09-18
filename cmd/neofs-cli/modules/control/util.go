package control

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
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

func signRequest(pk *ecdsa.PrivateKey, req controlSvc.SignedMessage) error {
	err := controlSvc.SignMessage(pk, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}
	return nil
}

func verifyResponse(sigControl interface {
	GetKey() []byte
	GetSign() []byte
}, body interface{ StableMarshal([]byte) []byte }) error {
	if sigControl == nil {
		return errors.New("missing response signature")
	}

	var pubKey neofsecdsa.PublicKey
	if err := pubKey.Decode(sigControl.GetKey()); err != nil {
		return fmt.Errorf("decode public key from signature: %w", err)
	}

	sig := neofscrypto.NewSignature(neofscrypto.ECDSA_SHA512, &pubKey, sigControl.GetSign())

	if !sig.Verify(body.StableMarshal(nil)) {
		return errors.New("invalid response signature")
	}
	return nil
}

func getClient(ctx context.Context) (*client.Client, error) {
	return internalclient.GetSDKClientByFlag(ctx, controlRPC)
}
