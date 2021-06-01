package grpcconfig

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/stretchr/testify/require"
)

func TestGRPCSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		tlsEnabled := TLS(empty).Enabled()

		require.Equal(t, false, tlsEnabled)

		require.PanicsWithError(
			t,
			errEndpointNotSet.Error(),
			func() {
				Endpoint(empty)
			},
		)

		require.PanicsWithError(
			t,
			errTLSKeyNotSet.Error(),
			func() {
				TLS(empty).KeyFile()
			},
		)

		require.PanicsWithError(
			t,
			errTLSCertNotSet.Error(),
			func() {
				TLS(empty).CertificateFile()
			},
		)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		addr := Endpoint(c)
		tlsEnabled := TLS(c).Enabled()
		tlsCert := TLS(c).CertificateFile()
		tlsKey := TLS(c).KeyFile()

		require.Equal(t, "s01.neofs.devenv:8080", addr)
		require.Equal(t, true, tlsEnabled)
		require.Equal(t, "/path/to/cert", tlsCert)
		require.Equal(t, "/path/to/key", tlsKey)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
