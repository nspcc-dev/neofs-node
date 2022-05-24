package control

import (
	"crypto/ecdsa"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/spf13/cobra"
)

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
		StableMarshal([]byte) ([]byte, error)
	},
) {
	if sigControl == nil {
		common.ExitOnErr(cmd, "", errors.New("missing response signature"))
	}

	bodyData, err := body.StableMarshal(nil)
	common.ExitOnErr(cmd, "marshal response body: %w", err)

	// TODO(@cthulhu-rider): #1387 use Signature message from NeoFS API to avoid conversion
	var sigV2 refs.Signature
	sigV2.SetScheme(refs.ECDSA_SHA512)
	sigV2.SetKey(sigControl.GetKey())
	sigV2.SetSign(sigControl.GetSign())

	var sig neofscrypto.Signature
	sig.ReadFromV2(sigV2)

	if !sig.Verify(bodyData) {
		common.ExitOnErr(cmd, "", errors.New("invalid response signature"))
	}
}

func getClient(cmd *cobra.Command, pk *ecdsa.PrivateKey) *client.Client {
	return internalclient.GetSDKClientByFlag(cmd, pk, controlRPC)
}
