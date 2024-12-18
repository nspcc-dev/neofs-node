package control

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
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

	addr, withTLS, err := parseURI(endpoint)
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

// parses URI and returns a host and a flag indicating that TLS is enabled.
// Copy-paste from https://github.com/nspcc-dev/neofs-sdk-go/blob/0b9bb748ea16370c9b88090a902413324e86bcf1/internal/uriutil/uri.go#L13.
func parseURI(s string) (string, bool, error) {
	uri, err := url.ParseRequestURI(s)
	if err != nil {
		if !strings.Contains(s, "/") {
			_, _, err := net.SplitHostPort(s)
			return s, false, err
		}
		return s, false, err
	}

	const (
		grpcScheme    = "grpc"
		grpcTLSScheme = "grpcs"
	)

	// check if passed string was parsed correctly
	// URIs that do not start with a slash after the scheme are interpreted as:
	// `scheme:opaque` => if `opaque` is not empty, then it is supposed that URI
	// is in `host:port` format
	if uri.Host == "" {
		uri.Host = uri.Scheme
		uri.Scheme = grpcScheme // assume GRPC by default
		if uri.Opaque != "" {
			uri.Host = net.JoinHostPort(uri.Host, uri.Opaque)
		}
	}

	switch uri.Scheme {
	case grpcTLSScheme, grpcScheme:
	default:
		return "", false, fmt.Errorf("unsupported scheme: %s", uri.Scheme)
	}

	if uri.Port() == "" {
		return "", false, errors.New("missing port in address")
	}

	return uri.Host, uri.Scheme == grpcTLSScheme, nil
}
