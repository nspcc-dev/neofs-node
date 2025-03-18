package validate

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-node/cmd/internal/configvalidator"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestCheckForUnknownFields(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "with all right fields",
			config: `
node:
  wallet:
    path: "./wallet.json"
    address: "NcpJzXcSDrh5CCizf4K9Ro6w4t59J5LKzz"
    password: "password"
  addresses:
    - s01.neofs.devenv:8080
    - /dns4/s02.neofs.devenv/tcp/8081
    - grpc://127.0.0.1:8082
    - grpcs://localhost:8083
  attribute_0: "Price:11"
  attribute_1: UN-LOCODE:RU MSK
  attribute_2: VerifiedNodesDomain:nodes.some-org.neofs
  relay: true
  persistent_sessions:
    path: /sessions
  persistent_state:
    path: /state
`,
			wantErr: false,
		},
		{
			name: "unknown node.password",
			config: `
node:
  wallet:
    path: "./wallet.json"
    address: "NcpJzXcSDrh5CCizf4K9Ro6w4t59J5LKzz"
  password: "password"
  addresses:
    - s01.neofs.devenv:8080
    - /dns4/s02.neofs.devenv/tcp/8081
    - grpc://127.0.0.1:8082
    - grpcs://localhost:8083
  attribute_0: "Price:11"
  attribute_1: UN-LOCODE:RU MSK
  attribute_2: VerifiedNodesDomain:nodes.some-org.neofs
  relay: true
  persistent_sessions:
    path: /sessions
  persistent_state:
    path: /state
`,
			wantErr: true,
		},
		{
			name: "node.wallet.address expected type string",
			config: `
node:
  wallet:
    path: "./wallet.json"
    address: 
      password: "password"
  addresses: s01.neofs.devenv:8080
  attribute_0: "Price:11"
  attribute_1: UN-LOCODE:RU MSK
  attribute_2: VerifiedNodesDomain:nodes.some-org.neofs
  relay: true
  persistent_sessions:
    path: /sessions
  persistent_state:
    path: /state
`,
			wantErr: true,
		},
		{
			name: "unknown field node.attr",
			config: `
node:
  wallet:
    path: "./wallet.json"
    address: "NcpJzXcSDrh5CCizf4K9Ro6w4t59J5LKzz"
    password: "password"
  addresses: s01.neofs.devenv:8080
  attribute_0: "Price:11"
  attribute_1: UN-LOCODE:RU MSK
  attribute_2: VerifiedNodesDomain:nodes.some-org.neofs
  attr: attr
  relay: true
  persistent_sessions:
    path: /sessions
  persistent_state:
    path: /state
`,
			wantErr: true,
		},
		{
			name: "grpc right",
			config: `
grpc:
  - endpoint: s01.neofs.devenv:8080
    conn_limit: 1
    tls:
      enabled: true
      certificate: /path/to/cert
      key: /path/to/key

  - endpoint: s02.neofs.devenv:8080
    conn_limit: -1
    tls:
      enabled: false
  - endpoint: s03.neofs.devenv:8080
`,
			wantErr: false,
		},
		{
			name: "unknown field grpc.key",
			config: `
grpc:
  - endpoint: s01.neofs.devenv:8080
    conn_limit: 1
    tls:
      enabled: true
      certificate: /path/to/cert
    key: /path/to/key

  - endpoint: s02.neofs.devenv:8080
    conn_limit: -1
    tls:
      enabled: false
  - endpoint: s03.neofs.devenv:8080
`,
			wantErr: true,
		},
		{
			name: "unknown field grpc.unknown",
			config: `
grpc:
  - endpoint: s01.neofs.devenv:8080
    conn_limit: 1
    tls:
      enabled: true
      certificate: /path/to/cert
      key: /path/to/key

  - endpoint: s02.neofs.devenv:8080
    conn_limit: -1
    tls:
      enabled: false
  - endpoint: s03.neofs.devenv:8080
  - unknown: field
`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := viper.New()
			v.SetConfigType("yaml")
			require.NoError(t, v.ReadConfig(strings.NewReader(tt.config)))

			err := ValidateStruct(v)
			fmt.Println(err)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckForUnknownFields() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckForUnknownFieldsExamples(t *testing.T) {
	const exampleConfigPrefix = "../../../../../config/"
	t.Run("example json", func(t *testing.T) {
		path := filepath.Join(exampleConfigPrefix, "example/node.json")
		v := viper.New()
		v.SetConfigFile(path)

		require.NoError(t, v.ReadInConfig())
		require.NoError(t, ValidateStruct(v))
	})

	t.Run("example yaml", func(t *testing.T) {
		path := filepath.Join(exampleConfigPrefix, "example/node.yaml")
		v := viper.New()
		v.SetConfigFile(path)

		require.NoError(t, v.ReadInConfig())
		require.NoError(t, ValidateStruct(v))
	})

	t.Run("mainnet", func(t *testing.T) {
		p := filepath.Join(exampleConfigPrefix, "mainnet/config.yml")
		v := viper.New()
		v.SetConfigFile(p)

		require.NoError(t, v.ReadInConfig())
		require.NoError(t, ValidateStruct(v))
	})
	t.Run("testnet", func(t *testing.T) {
		p := filepath.Join(exampleConfigPrefix, "testnet/config.yml")
		v := viper.New()
		v.SetConfigFile(p)

		require.NoError(t, v.ReadInConfig())
		require.NoError(t, ValidateStruct(v))
	})
}

func TestRemovedConfigurations(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")
	var y strings.Reader

	for _, tc := range []struct{ field, yaml string }{
		{field: "apiclient.allow_external", yaml: `
apiclient:
  allow_external: any
`},
	} {
		t.Run(tc.field, func(t *testing.T) {
			y.Reset(tc.yaml)
			require.NoError(t, v.ReadConfig(&y))
			err := ValidateStruct(v)
			require.ErrorIs(t, err, configvalidator.ErrUnknowField)
			require.EqualError(t, err, fmt.Sprintf("unknown field: %s", tc.field))
		})
	}
}
