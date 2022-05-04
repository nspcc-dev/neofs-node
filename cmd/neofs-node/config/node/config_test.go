package nodeconfig

import (
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	configtest "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/test"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/stretchr/testify/require"
)

func TestNodeSection(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		empty := configtest.EmptyConfig()

		require.Panics(
			t,
			func() {
				Key(empty)
			},
		)

		require.Panics(
			t,
			func() {
				BootstrapAddresses(empty)
			},
		)

		attribute := Attributes(empty)
		relay := Relay(empty)
		persisessionsPath := PersistentSessions(empty).Path()
		persistatePath := PersistentState(empty).Path()
		notificationDefaultEnabled := Notification(empty).Enabled()
		notificationDefaultEndpoint := Notification(empty).Endpoint()
		notificationDefaultTimeout := Notification(empty).Timeout()
		notificationDefaultTopic := Notification(empty).DefaultTopic()
		notificationDefaultCertPath := Notification(empty).CertPath()
		notificationDefaultKeyPath := Notification(empty).KeyPath()
		notificationDefaultCAPath := Notification(empty).CAPath()

		require.Empty(t, attribute)
		require.Equal(t, false, relay)
		require.Equal(t, "", persisessionsPath)
		require.Equal(t, PersistentStatePathDefault, persistatePath)
		require.Equal(t, false, notificationDefaultEnabled)
		require.Equal(t, "", notificationDefaultEndpoint)
		require.Equal(t, NotificationTimeoutDefault, notificationDefaultTimeout)
		require.Equal(t, "", notificationDefaultTopic)
		require.Equal(t, "", notificationDefaultCertPath)
		require.Equal(t, "", notificationDefaultKeyPath)
		require.Equal(t, "", notificationDefaultCAPath)

		var subnetCfg SubnetConfig

		subnetCfg.Init(*empty)

		require.False(t, subnetCfg.ExitZero())

		called := false

		subnetCfg.IterateSubnets(func(string) {
			called = true
		})

		require.False(t, called)
	})

	const path = "../../../../config/example/node"

	var fileConfigTest = func(c *config.Config) {
		key := Key(c)
		addrs := BootstrapAddresses(c)
		attributes := Attributes(c)
		relay := Relay(c)
		wKey := Wallet(c)
		persisessionsPath := PersistentSessions(c).Path()
		persistatePath := PersistentState(c).Path()
		notificationEnabled := Notification(c).Enabled()
		notificationEndpoint := Notification(c).Endpoint()
		notificationTimeout := Notification(c).Timeout()
		notificationDefaultTopic := Notification(c).DefaultTopic()
		notificationCertPath := Notification(c).CertPath()
		notificationKeyPath := Notification(c).KeyPath()
		notificationCAPath := Notification(c).CAPath()

		expectedAddr := []struct {
			str  string
			host string
		}{
			{
				str:  "/dns4/localhost/tcp/8083/tls",
				host: "grpcs://localhost:8083",
			},
			{
				str:  "/dns4/s01.neofs.devenv/tcp/8080",
				host: "s01.neofs.devenv:8080",
			},
			{
				str:  "/dns4/s02.neofs.devenv/tcp/8081",
				host: "s02.neofs.devenv:8081",
			},
			{
				str:  "/ip4/127.0.0.1/tcp/8082",
				host: "127.0.0.1:8082",
			},
		}

		require.Equal(t, "NbUgTSFvPmsRxmGeWpuuGeJUoRoi6PErcM", key.Address())

		require.EqualValues(t, len(expectedAddr), addrs.Len())

		ind := 0

		addrs.IterateAddresses(func(addr network.Address) bool {
			require.Equal(t, expectedAddr[ind].str, addr.String())
			require.Equal(t, expectedAddr[ind].host, addr.URIAddr())

			ind++

			return false
		})

		require.Equal(t, true, relay)

		require.Len(t, attributes, 2)
		require.Equal(t, "Price:11", attributes[0])
		require.Equal(t, "UN-LOCODE:RU MSK", attributes[1])

		require.NotNil(t, wKey)
		require.Equal(t,
			config.StringSafe(c.Sub("node").Sub("wallet"), "address"),
			address.Uint160ToString(wKey.GetScriptHash()))

		require.Equal(t, "/sessions", persisessionsPath)
		require.Equal(t, "/state", persistatePath)
		require.Equal(t, true, notificationEnabled)
		require.Equal(t, "tls://localhost:4222", notificationEndpoint)
		require.Equal(t, 6*time.Second, notificationTimeout)
		require.Equal(t, "topic", notificationDefaultTopic)
		require.Equal(t, "/cert/path", notificationCertPath)
		require.Equal(t, "/key/path", notificationKeyPath)
		require.Equal(t, "/ca/path", notificationCAPath)

		var subnetCfg SubnetConfig

		subnetCfg.Init(*c)

		require.True(t, subnetCfg.ExitZero())

		var ids []string

		subnetCfg.IterateSubnets(func(id string) {
			ids = append(ids, id)
		})

		require.Equal(t, []string{"123", "456", "789"}, ids)
	}

	configtest.ForEachFileType(path, fileConfigTest)

	t.Run("ENV", func(t *testing.T) {
		configtest.ForEnvFileType(path, fileConfigTest)
	})
}
