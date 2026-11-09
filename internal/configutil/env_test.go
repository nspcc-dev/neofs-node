package configutil_test

import (
	"strings"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/configutil"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

var yamlSimpleSlice = []byte(`
name: Steve
port: 8080
auth:
  secret: 88888-88888
modes:
  - 1
  - 2
  - 3
clients:
  - name: foo
  - name: bar
proxy:
  clients:
  - name: proxy_foo
  - name: proxy_bar
  - name: proxy_baz
`)

func TestEnvUnmarshal(t *testing.T) {
	v := viper.New()
	v.SetConfigType("yaml")

	err := v.ReadConfig(strings.NewReader(string(yamlSimpleSlice)))
	require.NoError(t, err)

	require.Equal(t, "Steve", v.GetString("name"))
	require.Equal(t, 8080, v.GetInt("port"))
	require.Equal(t, "88888-88888", v.GetString("auth.secret"))
	require.Equal(t, "foo", v.GetString("clients.0.name"))
	require.Equal(t, "bar", v.GetString("clients.1.name"))
	require.Equal(t, "proxy_foo", v.GetString("proxy.clients.0.name"))
	require.Equal(t, []int{1, 2, 3}, v.GetIntSlice("modes"))

	// Override with env variable
	t.Setenv("APP_NAME", "Steven")
	t.Setenv("APP_AUTH_SECRET", "99999-99999")
	t.Setenv("APP_MODES_2", "300")
	t.Setenv("APP_CLIENTS_1_NAME", "baz")
	t.Setenv("APP_PROXY_CLIENTS_0_NAME", "ProxyFoo")
	t.Setenv("APP_PROXY_CLIENTS_3_NAME", "ProxyNew")

	const envPrefix = "APP"
	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Unmarshal into struct
	var cfg config
	err = configutil.Unmarshal(v, &cfg, envPrefix)
	require.NoError(t, err)

	require.Equal(t, "Steven", cfg.Name)
	require.Equal(t, 8080, cfg.Port)
	require.Equal(t, "99999-99999", cfg.Auth.Secret)
	require.Equal(t, []int{1, 2, 300}, cfg.Modes)
	require.Equal(t, "foo", cfg.Clients[0].Name)
	require.Equal(t, "baz", cfg.Clients[1].Name)
	require.Equal(t, "ProxyFoo", cfg.Proxy.Clients[0].Name)
	require.Equal(t, "ProxyNew", cfg.Proxy.Clients[3].Name)
}

// A struct-slice field (clients) given as a scalar instead of a list in the
// config (e.g. a typo, or a list mistakenly indented as a single value) must
// be rejected with a clean error from Unmarshal, not a panic - this is
// reachable not just at startup but via SIGHUP config reload on a running
// node. Before the fix, Get-ing the misshapen value and asserting it directly
// to []any paniced with "interface conversion: interface {} is string, not
// []any" instead of reaching mapstructure's own, properly error-returning
// type check.
//
// The env-override subtests matter separately: an override makes
// processStructSlice rebuild the list and the caller v.Set it back over the
// original, so a malformed value that is merely tolerated there (rather than
// bailing out) would be overwritten and never reach that type check - the
// config would load with a zero-valued entry and no error at all.
func TestEnvUnmarshal_StructSliceWrongShape(t *testing.T) {
	const wantErr = `'clients[1]' expected a map or struct, got "string"`

	t.Run("scalar instead of list", func(t *testing.T) {
		require.Error(t, unmarshalShape(t, "clients: oops\n"))
	})

	t.Run("scalar instead of list, with env override", func(t *testing.T) {
		t.Setenv("APP_CLIENTS_0_NAME", "overridden")
		require.Error(t, unmarshalShape(t, "clients: oops\n"))
	})

	t.Run("malformed item in an otherwise valid list", func(t *testing.T) {
		err := unmarshalShape(t, "clients:\n  - name: foo\n  - oops\n")
		require.ErrorContains(t, err, wantErr)
	})

	t.Run("malformed item, with env override", func(t *testing.T) {
		t.Setenv("APP_CLIENTS_0_NAME", "overridden")
		err := unmarshalShape(t, "clients:\n  - name: foo\n  - oops\n")
		require.ErrorContains(t, err, wantErr)
	})
}

// unmarshalShape reads yml and unmarshals it, asserting only that no panic
// occurs; the returned error is left to the caller to check.
func unmarshalShape(t *testing.T, yml string) error {
	t.Helper()

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(yml)))

	var (
		cfg config
		err error
	)
	require.NotPanics(t, func() {
		err = configutil.Unmarshal(v, &cfg, "APP")
	})

	return err
}

type clientConfig struct {
	Name string `mapstructure:"name"`
}

type config struct {
	Port int    `mapstructure:"port"`
	Name string `mapstructure:"name"`
	Auth struct {
		Secret string `mapstructure:"secret"`
	} `mapstructure:"auth"`
	Modes   []int          `mapstructure:"modes"`
	Clients []clientConfig `mapstructure:"clients"`
	Proxy   struct {
		Clients []clientConfig `mapstructure:"clients"`
	} `mapstructure:"proxy"`
}

func (c *config) Set(string)   {}
func (c *config) Unset(string) {}
