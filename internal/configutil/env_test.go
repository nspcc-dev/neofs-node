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
