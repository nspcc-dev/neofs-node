package innerring

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// YAML configuration of the IR consensus with all required fields.
const validBlockchainConfigMinimal = `
fschain:
  consensus:
    magic: 15405
    committee:
      - 02cddc58c3f7d27b5c9967dd90fbd4269798cbbb9cd7b137d886aca209cb734fb6
      - 03f87b0a0416e4028bccf7258db3b411412ce1c7426b2c857f54e59d0d23782570
      - 03d90c07df63e690ce77912e10ab51acc944b66860237b608c4f8f8309e71ee699
      - 02a7bc55fe8684e0119768d104ba30795bdcc86619e864add26156723ed185cd62
    storage:
      type: boltdb
      path: chain.db
`

// YAML sub-configuration of the IR consensus with all optional fields.
const validBlockchainConfigOptions = `
    time_per_block: 1s
    max_traceable_blocks: 200
    seed_nodes:
      - localhost:20000
      - localhost:20001
      - localhost
    hardforks:
      name: 1730000
    validators_history:
      0: 4
      4: 1
      12: 4
    rpc:
      max_websocket_clients: 100
      session_pool_size: 100
      listen:
        - localhost:30000
        - localhost:30001
        - localhost
      tls:
        enabled: true
        listen:
          - localhost:30002
          - localhost:30003
          - localhost
        cert_file: /path/to/cert
        key_file: /path/to/key
    p2p:
      dial_timeout: 111s
      proto_tick_interval: 222s
      listen:
        - localhost:20100
        - localhost:20101
        - localhost
      peers:
        min: 1
        max: 5
        attempts: 20 # How many peers node should try to dial after falling under 'min' count. Must be in range [1:2147483647]
      ping:
        interval: 44s
        timeout: 55s
    set_roles_in_genesis: true
    keep_only_latest_state: true
    remove_untraceable_blocks: true
    p2p_notary_request_payload_pool_size: 100
`

func _newConfigFromYAML(tb testing.TB, yaml1, yaml2 string) *viper.Viper {
	v := viper.New()
	v.SetConfigType("yaml")

	err := v.ReadConfig(strings.NewReader(yaml1 + yaml2))
	require.NoError(tb, err)

	return v
}

// returns viper.Viper initialized from valid blockchain configuration above.
func newValidBlockchainConfig(tb testing.TB, full bool) *viper.Viper {
	if full {
		return _newConfigFromYAML(tb, validBlockchainConfigMinimal, validBlockchainConfigOptions)
	}

	return _newConfigFromYAML(tb, validBlockchainConfigMinimal, "")
}

// resets value by key. Currently, viper doesn't provide unset method. Here is a
// workaround suggested in https://github.com/spf13/viper/issues/632.
func resetConfig(tb testing.TB, v *viper.Viper, key string) {
	mAll := v.AllSettings()
	mAllCp := mAll

	parts := strings.Split(key, ".")
	for i, k := range parts {
		v, ok := mAllCp[k]
		if !ok {
			// Doesn't exist no action needed
			break
		}

		switch len(parts) {
		case i + 1:
			// Last part so delete.
			delete(mAllCp, k)
		default:
			m, ok := v.(map[string]any)
			require.Truef(tb, ok, "unsupported type: %T for %q", v, strings.Join(parts[0:i], "."))
			mAllCp = m
		}
	}

	*v = *viper.New()

	for key, val := range mAll {
		v.Set(key, val)
	}
}

func commiteeN(t testing.TB, n int) []string {
	res := make([]string, n)
	for i := range res {
		k, err := keys.NewPrivateKey()
		require.NoError(t, err)
		res[i] = k.PublicKey().StringCompressed()
	}
	return res
}

func TestParseBlockchainConfig(t *testing.T) {
	fullConfig := true
	_logger := zap.NewNop()

	validCommittee, err := keys.NewPublicKeysFromStrings([]string{
		"02cddc58c3f7d27b5c9967dd90fbd4269798cbbb9cd7b137d886aca209cb734fb6",
		"03f87b0a0416e4028bccf7258db3b411412ce1c7426b2c857f54e59d0d23782570",
		"03d90c07df63e690ce77912e10ab51acc944b66860237b608c4f8f8309e71ee699",
		"02a7bc55fe8684e0119768d104ba30795bdcc86619e864add26156723ed185cd62",
	})
	require.NoError(t, err)

	t.Run("minimal", func(t *testing.T) {
		v := newValidBlockchainConfig(t, !fullConfig)
		c, err := parseBlockchainConfig(v, _logger)
		require.NoError(t, err)

		require.Equal(t, blockchain.Config{
			Logger:       _logger,
			NetworkMagic: 15405,
			Committee:    validCommittee,
			Storage:      blockchain.BoltDB("chain.db"),
			P2P:          blockchain.P2PConfig{MinPeers: 2},
		}, c)
	})

	t.Run("full", func(t *testing.T) {
		v := newValidBlockchainConfig(t, fullConfig)
		c, err := parseBlockchainConfig(v, _logger)
		require.NoError(t, err)

		require.Equal(t, blockchain.Config{
			Logger:        _logger,
			NetworkMagic:  15405,
			Committee:     validCommittee,
			BlockInterval: time.Second,
			RPC: blockchain.RPCConfig{
				MaxWebSocketClients: 100,
				SessionPoolSize:     100,
				Addresses: []string{
					"localhost:30000",
					"localhost:30001",
					"localhost:30333",
				},
				TLSConfig: blockchain.TLSConfig{
					Enabled:  true,
					CertFile: "/path/to/cert",
					KeyFile:  "/path/to/key",
					Addresses: []string{
						"localhost:30002",
						"localhost:30003",
						"localhost:30333",
					},
				},
			},
			TraceableChainLength: 200,
			HardForks: map[string]uint32{
				"name": 1730000,
			},
			SeedNodes: []string{
				"localhost:20000",
				"localhost:20001",
				"localhost:20333",
			},
			P2P: blockchain.P2PConfig{
				MinPeers:         1,
				AttemptConnPeers: 20,
				MaxPeers:         5,
				Ping: blockchain.PingConfig{
					Interval: 44 * time.Second,
					Timeout:  55 * time.Second,
				},
				DialTimeout:       111 * time.Second,
				ProtoTickInterval: 222 * time.Second,
				ListenAddresses: []string{
					"localhost:20100",
					"localhost:20101",
					"localhost:20333",
				},
			},
			Storage: blockchain.BoltDB("chain.db"),
			ValidatorsHistory: map[uint32]uint32{
				0:  4,
				4:  1,
				12: 4,
			},
			SetRolesInGenesis: true,
			Ledger: blockchain.LedgerConfig{
				KeepOnlyLatestState:     true,
				RemoveUntraceableBlocks: true,
			},
			P2PNotaryRequestPayloadPoolSize: 100,
		}, c)
	})

	t.Run("incomplete", func(t *testing.T) {
		for _, requiredKey := range []string{
			"magic",
			"committee",
			"storage",
			"storage.type",
		} {
			v := newValidBlockchainConfig(t, !fullConfig)
			resetConfig(t, v, "fschain.consensus."+requiredKey)
			_, err := parseBlockchainConfig(v, _logger)
			require.Error(t, err, requiredKey)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		v := newValidBlockchainConfig(t, fullConfig)
		resetConfig(t, v, "fschain.consensus")
		_, err := parseBlockchainConfig(v, _logger)
		require.Error(t, err)

		type kv struct {
			key string
			val any
		}

		kvF := func(k string, v any) kv {
			return kv{k, v}
		}

		for _, testCase := range [][]kv{
			{kvF("magic", "not an integer")},
			{kvF("magic", -1)},
			{kvF("magic", 0)},
			{kvF("magic", 0.1)},
			{kvF("magic", math.MaxUint32+1)},
			{kvF("committee", []string{})},
			{kvF("committee", []string{"not a key"})},
			{kvF("storage.type", "random string")},
			{kvF("time_per_block", "not a duration")},
			{kvF("time_per_block", -time.Second)},
			{kvF("max_traceable_blocks", -1)},
			{kvF("max_traceable_blocks", math.MaxUint32+1)},
			{kvF("seed_nodes", []string{"not a TCP address"})},
			{kvF("hardforks", "not a dictionary")},
			{kvF("hardforks", map[string]any{"": 1})},
			{kvF("hardforks", map[string]any{"name": "not a number"})},
			{kvF("hardforks", map[string]any{"name": -1})},
			{kvF("hardforks", map[string]any{"name": math.MaxUint32 + 1})},
			{kvF("validators_history", map[string]any{"not a number": 1})},
			{kvF("validators_history", map[string]any{"0": "not a number"})},
			{kvF("validators_history", map[string]any{"-1": 1})},
			{kvF("validators_history", map[string]any{"0": -1})},
			{kvF("validators_history", map[string]any{"0": math.MaxUint32 + 1})},
			{kvF("validators_history", map[string]any{"0": len(validCommittee) + 1})},
			{kvF("validators_history", map[string]any{"0": 1, "3": 1})}, // height is not a multiple
			{kvF("rpc.listen", []string{"not a TCP address"})},
			{kvF("rpc.max_websocket_clients", -1)},
			{kvF("rpc.max_websocket_clients", math.MaxInt32+1)},
			{kvF("rpc.session_pool_size", -1)},
			{kvF("rpc.session_pool_size", math.MaxInt32+1)},
			{kvF("rpc.tls.enabled", true), kvF("rpc.tls.cert_file", "")},                                       // enabled but no cert file is provided
			{kvF("rpc.tls.enabled", true), kvF("rpc.tls.cert_file", " \t")},                                    // enabled but no but blank cert is provided
			{kvF("rpc.tls.enabled", true), kvF("rpc.tls.cert_file", "/path/"), kvF("rpc.tls.key_file", "")},    // enabled but no key is provided
			{kvF("rpc.tls.enabled", true), kvF("rpc.tls.cert_file", "/path/"), kvF("rpc.tls.key_file", " \t")}, // enabled but no but blank key is provided
			{kvF("p2p.listen", []string{"not a TCP address"})},
			{kvF("p2p.dial_timeout", "not a duration")},
			{kvF("p2p.dial_timeout", -time.Second)},
			{kvF("p2p.proto_tick_interval", "not a duration")},
			{kvF("p2p.proto_tick_interval", -time.Second)},
			{kvF("p2p.ping.interval", "not a duration")},
			{kvF("p2p.ping.interval", -time.Second)},
			{kvF("p2p.ping.timeout", "not a duration")},
			{kvF("p2p.ping.timeout", -time.Second)},
			{kvF("p2p.peers.min", -1)},
			{kvF("p2p.peers.min", math.MaxInt32+1)},
			{kvF("p2p.peers.max", -1)},
			{kvF("p2p.peers.max", math.MaxInt32+1)},
			{kvF("p2p.peers.attempts", -1)},
			{kvF("p2p.peers.attempts", math.MaxInt32+1)},
			{kvF("set_roles_in_genesis", "not a boolean")},
			{kvF("set_roles_in_genesis", 1)},
			{kvF("set_roles_in_genesis", "True")},
			{kvF("set_roles_in_genesis", "False")},
			{kvF("set_roles_in_genesis", "not a boolean")},
			{kvF("p2p_notary_request_payload_pool_size", -1)},
			{kvF("keep_only_latest_state", 1)},
			{kvF("keep_only_latest_state", "True")},
			{kvF("remove_untraceable_blocks", 1)},
			{kvF("remove_untraceable_blocks", "True")},
			{kvF("p2p_notary_request_payload_pool_size", math.MaxUint32+1)},
		} {
			var reportMsg []string

			v := newValidBlockchainConfig(t, fullConfig)
			for _, kvPair := range testCase {
				key := kvPair.key
				val := kvPair.val

				v.Set("fschain.consensus."+key, val)
				reportMsg = append(reportMsg, fmt.Sprintf("%s=%v", key, val))
			}

			_, err := parseBlockchainConfig(v, _logger)
			require.Error(t, err, strings.Join(reportMsg, ", "))
		}
	})

	t.Run("enums", func(t *testing.T) {
		t.Run("storage", func(t *testing.T) {
			v := newValidBlockchainConfig(t, fullConfig)
			const path = "path/to/db"

			v.Set("fschain.consensus.storage.path", path)
			v.Set("fschain.consensus.storage.type", "boltdb")
			c, err := parseBlockchainConfig(v, _logger)
			require.NoError(t, err)
			require.Equal(t, blockchain.BoltDB(path), c.Storage)

			resetConfig(t, v, "fschain.consensus.storage.path")
			_, err = parseBlockchainConfig(v, _logger)
			require.Error(t, err)

			v.Set("fschain.consensus.storage.path", path)
			v.Set("fschain.consensus.storage.type", "leveldb")
			c, err = parseBlockchainConfig(v, _logger)
			require.NoError(t, err)
			require.Equal(t, blockchain.LevelDB(path), c.Storage)

			resetConfig(t, v, "fschain.consensus.storage.path")
			_, err = parseBlockchainConfig(v, _logger)
			require.Error(t, err)

			// no path needed
			v.Set("fschain.consensus.storage.type", "inmemory")
			c, err = parseBlockchainConfig(v, _logger)
			require.NoError(t, err)
			require.Equal(t, blockchain.InMemory(), c.Storage)
		})
	})
}

func TestIsLocalConsensusMode(t *testing.T) {
	t.Run("ENV", func(t *testing.T) {
		v := viper.New()
		v.AutomaticEnv()
		v.SetEnvPrefix("neofs_ir")
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

		const envKeyEndpoints = "NEOFS_IR_FSCHAIN_ENDPOINTS"
		const envKeyConsensus = "NEOFS_IR_FSCHAIN_CONSENSUS"
		var err error

		for _, tc := range []struct {
			setEndpoints bool
			setConsensus bool
			expected     uint8 // 0:false, 1:true, 2:error
		}{
			{
				setEndpoints: true,
				setConsensus: true,
				expected:     0,
			},
			{
				setEndpoints: true,
				setConsensus: false,
				expected:     0,
			},
			{
				setEndpoints: false,
				setConsensus: true,
				expected:     1,
			},
			{
				setEndpoints: false,
				setConsensus: false,
				expected:     2,
			},
		} {
			if tc.setEndpoints {
				err = os.Setenv(envKeyEndpoints, "any")
			} else {
				err = os.Unsetenv(envKeyEndpoints)
			}
			require.NoError(t, err, tc)

			if tc.setConsensus {
				err = os.Setenv(envKeyConsensus, "any")
			} else {
				err = os.Unsetenv(envKeyConsensus)
			}
			require.NoError(t, err, tc)

			res, err := isLocalConsensusMode(v)
			switch tc.expected {
			default:
				t.Fatalf("unexpected result value %v", tc.expected)
			case 0:
				require.NoError(t, err, tc)
				require.False(t, res, tc)
			case 1:
				require.NoError(t, err, tc)
				require.True(t, res, tc)
			case 2:
				require.Error(t, err, tc)
			}
		}
	})

	t.Run("YAML", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")
		err := v.ReadConfig(strings.NewReader(`
fschain:
  endpoints:
      - ws://fs-chain:30333/ws
`))
		require.NoError(t, err)

		res, err := isLocalConsensusMode(v)
		require.NoError(t, err)
		require.False(t, res)

		resetConfig(t, v, "fschain.endpoints")

		_, err = isLocalConsensusMode(v)
		require.Error(t, err)

		v.Set("fschain.consensus", "any")

		res, err = isLocalConsensusMode(v)
		require.NoError(t, err)
		require.True(t, res)
	})
}

// YAML configuration of the NNS with all required fields.
const validNNSConfig = `
nns:
  system_email: usr@domain.io
`

// returns viper.Viper initialized from valid NNS configuration above.
func newValidNNSConfig(tb testing.TB) *viper.Viper {
	return _newConfigFromYAML(tb, validNNSConfig, "")
}

func TestParseNNSConfig(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		c, err := parseNNSConfig(viper.New())
		require.NoError(t, err)

		require.Zero(t, c)
	})

	t.Run("minimal", func(t *testing.T) {
		v := newValidNNSConfig(t)
		c, err := parseNNSConfig(v)
		require.NoError(t, err)

		require.Equal(t, nnsConfig{
			systemEmail: "usr@domain.io",
		}, c)
	})

	t.Run("incomplete", func(t *testing.T) {
		for _, requiredKey := range []string{
			"system_email",
		} {
			v := newValidNNSConfig(t)
			resetConfig(t, v, "nns."+requiredKey)
			c, err := parseNNSConfig(v)
			require.NoError(t, err)
			require.Zero(t, c.systemEmail)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		type kv struct {
			key string
			val any
		}

		kvF := func(k string, v any) kv {
			return kv{k, v}
		}

		for _, testCase := range [][]kv{
			{kvF("system_email", map[string]any{"1": 2})}, // any non-string value
		} {
			var reportMsg []string

			v := newValidNNSConfig(t)
			for _, kvPair := range testCase {
				key := kvPair.key
				val := kvPair.val

				v.Set("nns."+key, val)
				reportMsg = append(reportMsg, fmt.Sprintf("%s=%v", key, val))
			}

			_, err := parseNNSConfig(v)
			require.Error(t, err, strings.Join(reportMsg, ", "))
		}
	})
}

func TestIsAutoDeploymentMode(t *testing.T) {
	t.Run("ENV", func(t *testing.T) {
		v := viper.New()
		v.AutomaticEnv()
		v.SetEnvPrefix("neofs_ir")
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

		const envKey = "NEOFS_IR_FSCHAIN_AUTODEPLOY"

		err := os.Unsetenv(envKey)
		require.NoError(t, err)

		b, err := isAutoDeploymentMode(v)
		require.NoError(t, err)
		require.False(t, b)

		err = os.Setenv(envKey, "true")
		require.NoError(t, err)

		b, err = isAutoDeploymentMode(v)
		require.NoError(t, err)
		require.True(t, b)

		err = os.Setenv(envKey, "not a boolean")
		require.NoError(t, err)

		_, err = isAutoDeploymentMode(v)
		require.Error(t, err)

		err = os.Setenv(envKey, "false")
		require.NoError(t, err)

		b, err = isAutoDeploymentMode(v)
		require.NoError(t, err)
		require.False(t, b)
	})

	t.Run("YAML", func(t *testing.T) {
		v := viper.New()
		v.SetConfigType("yaml")
		err := v.ReadConfig(strings.NewReader(`
fschain_autodeploy: true
`))
		require.NoError(t, err)

		b, err := isAutoDeploymentMode(v)
		require.NoError(t, err)
		require.True(t, b)

		resetConfig(t, v, "fschain_autodeploy")

		b, err = isAutoDeploymentMode(v)
		require.NoError(t, err)
		require.False(t, b)

		v.Set("fschain_autodeploy", "not a boolean")

		_, err = isAutoDeploymentMode(v)
		require.Error(t, err)

		v.Set("fschain_autodeploy", "false")

		b, err = isAutoDeploymentMode(v)
		require.NoError(t, err)
		require.False(t, b)
	})
}

func TestP2PMinPeers(t *testing.T) {
	l := zap.NewNop()
	assert := func(t testing.TB, v *viper.Viper, exp uint) {
		c, err := parseBlockchainConfig(v, l)
		require.NoError(t, err)
		require.EqualValues(t, exp, c.P2P.MinPeers)
	}

	v := newValidBlockchainConfig(t, true)
	v.Set("fschain.consensus.p2p.peers.min", 123)
	assert(t, v, 123)

	t.Run("explicit zero", func(t *testing.T) {
		v := newValidBlockchainConfig(t, false)
		v.Set("fschain.consensus.p2p.peers.min", 0)
		assert(t, v, 0)
	})
	t.Run("default", func(t *testing.T) {
		assertDefault := func(t testing.TB, v *viper.Viper) {
			setCommitteeN := func(n int) {
				v.Set("fschain.consensus.committee", commiteeN(t, n))
				resetConfig(t, v, "fschain.consensus.validators_history") // checked against committee size
			}
			setCommitteeN(4)
			assert(t, v, 2)
			setCommitteeN(7)
			assert(t, v, 4)
			setCommitteeN(21)
			assert(t, v, 14)
		}
		t.Run("missing P2P section", func(t *testing.T) {
			v := newValidBlockchainConfig(t, true)
			resetConfig(t, v, "fschain.consensus.p2p")
			assertDefault(t, v)
		})
		t.Run("missing peers section", func(t *testing.T) {
			v := newValidBlockchainConfig(t, true)
			resetConfig(t, v, "fschain.consensus.p2p.peers")
			require.True(t, v.IsSet("fschain.consensus.p2p"))
			assertDefault(t, v)
		})
		t.Run("missing config itself", func(t *testing.T) {
			v := newValidBlockchainConfig(t, true)
			resetConfig(t, v, "fschain.consensus.p2p.peers.min")
			require.True(t, v.IsSet("fschain.consensus.p2p.peers"))
			assertDefault(t, v)
		})
	})
}
