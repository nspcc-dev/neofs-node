package main

import (
	"testing"

	grpcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/grpc"
	"github.com/stretchr/testify/require"
)

func TestClientCertificateProvider(t *testing.T) {
	require.Nil(t, clientCertificateProvider(nil))

	provider := clientCertificateProvider([]grpcconfig.GRPC{{
		TLS: grpcconfig.TLS{
			Enabled:     true,
			Certificate: "missing-certificate",
			Key:         "missing-key",
		},
	}})
	require.NotNil(t, provider)
	_, err := provider(nil)
	require.ErrorContains(t, err, "reload TLS client certificate")
}
