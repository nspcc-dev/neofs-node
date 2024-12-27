package control

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/internal/uriutil"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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

func getClient(ctx context.Context) (control.ControlServiceClient, error) {
	conn, err := connect(ctx)
	if err != nil {
		return nil, err
	}
	return control.NewControlServiceClient(conn), nil
}

func connect(ctx context.Context) (*grpc.ClientConn, error) {
	endpoint := viper.GetString(controlRPC)
	if endpoint == "" {
		return nil, fmt.Errorf("empty flag %q", controlRPC)
	}

	addr, withTLS, err := uriutil.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid %q flag: %w", controlRPC, err)
	}

	if _, ok := ctx.Deadline(); !ok { // otherwise global deadline is specified already, don't go over it
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	var creds credentials.TransportCredentials
	if withTLS {
		creds = credentials.NewTLS(nil)
	} else {
		creds = insecure.NewCredentials()
	}

	// TODO: copy-pasted from SDK. Replace deprecated func with
	//  grpc.NewClient. This was not done because some options are no longer
	//  supported. Review carefully and make a proper transition.
	//nolint:staticcheck
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithReturnConnectionError(),
		grpc.FailOnNonTempDialError(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %q via gRPC: %w", endpoint, err)
	}
	return conn, nil
}
