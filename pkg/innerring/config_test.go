package innerring

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/internal/configutil"
	irconfig "github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const irEnvPrefix = "neofs_ir"

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
      max_gas_invoke: 200
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

func unmarshalConfig(tb testing.TB, v *viper.Viper) *irconfig.Config {
	var cfg irconfig.Config
	err := configutil.Unmarshal(v, &cfg, irEnvPrefix)
	require.NoError(tb, err)
	return &cfg
}

func _newConfigFromYAML(tb testing.TB, yaml1, yaml2 string) *irconfig.Config {
	v := viper.New()
	v.SetConfigType("yaml")

	err := v.ReadConfig(strings.NewReader(yaml1 + yaml2))
	require.NoError(tb, err)

	return unmarshalConfig(tb, v)
}

// returns viper.Viper initialized from valid blockchain configuration above.
func newValidBlockchainConfig(tb testing.TB, full bool) *irconfig.Config {
	if full {
		return _newConfigFromYAML(tb, validBlockchainConfigMinimal, validBlockchainConfigOptions)
	}

	return _newConfigFromYAML(tb, validBlockchainConfigMinimal, "")
}

// resets value by key.
func resetConfig(tb testing.TB, field any) {
	v := reflect.ValueOf(field)

	if v.Kind() != reflect.Ptr {
		tb.Fatal("ResetField: argument must be a pointer")
	}

	elem := v.Elem()

	elem.Set(reflect.Zero(elem.Type()))
}

func commiteeN(t testing.TB, n int) keys.PublicKeys {
	res := make(keys.PublicKeys, n)
	for i := range res {
		k, err := keys.NewPrivateKey()
		require.NoError(t, err)
		res[i] = k.PublicKey()
	}
	return res
}

func TestParseBlockchainConfig(t *testing.T) {
	fullConfig := true

	validCommittee, err := keys.NewPublicKeysFromStrings([]string{
		"02cddc58c3f7d27b5c9967dd90fbd4269798cbbb9cd7b137d886aca209cb734fb6",
		"03f87b0a0416e4028bccf7258db3b411412ce1c7426b2c857f54e59d0d23782570",
		"03d90c07df63e690ce77912e10ab51acc944b66860237b608c4f8f8309e71ee699",
		"02a7bc55fe8684e0119768d104ba30795bdcc86619e864add26156723ed185cd62",
	})
	require.NoError(t, err)

	t.Run("minimal", func(t *testing.T) {
		cfg := newValidBlockchainConfig(t, !fullConfig)

		err := validateBlockchainConfig(cfg)
		require.NoError(t, err)

		cfg.UnsetAll()
		require.Equal(t, &irconfig.Config{
			FSChain: irconfig.Chain{
				Consensus: irconfig.Consensus{
					Magic:     15405,
					Committee: validCommittee,
					Storage: irconfig.Storage{
						Type: dbconfig.BoltDB,
						Path: "chain.db",
					},
					P2P: irconfig.P2P{
						Peers: irconfig.Peers{Min: 2}},
				},
			},
		}, cfg)
	})

	t.Run("full", func(t *testing.T) {
		cfg := newValidBlockchainConfig(t, fullConfig)
		err := validateBlockchainConfig(cfg)
		require.NoError(t, err)

		cfg.UnsetAll()
		require.Equal(t, &irconfig.Config{
			FSChain: irconfig.Chain{
				Consensus: irconfig.Consensus{
					Magic:        15405,
					Committee:    validCommittee,
					TimePerBlock: time.Second,
					RPC: irconfig.RPC{
						MaxWebSocketClients: 100,
						SessionPoolSize:     100,
						MaxGasInvoke:        200,
						Listen: []string{
							"localhost:30000",
							"localhost:30001",
							"localhost:30333",
						},
						TLS: irconfig.TLS{
							Enabled:  true,
							CertFile: "/path/to/cert",
							KeyFile:  "/path/to/key",
							Listen: []string{
								"localhost:30002",
								"localhost:30003",
								"localhost:30333",
							},
						},
					},
					MaxTraceableBlocks: 200,
					Hardforks: irconfig.Hardforks{
						Name: map[string]uint32{
							"name": 1730000,
						},
					},
					SeedNodes: []string{
						"localhost:20000",
						"localhost:20001",
						"localhost:20333",
					},
					P2P: irconfig.P2P{
						Peers: irconfig.Peers{
							Min:      1,
							Max:      5,
							Attempts: 20,
						},
						Ping: irconfig.Ping{
							Interval: 44 * time.Second,
							Timeout:  55 * time.Second,
						},
						DialTimeout:       111 * time.Second,
						ProtoTickInterval: 222 * time.Second,
						Listen: []string{
							"localhost:20100",
							"localhost:20101",
							"localhost:20333",
						},
					},
					Storage: irconfig.Storage{
						Type: dbconfig.BoltDB,
						Path: "chain.db",
					},
					ValidatorsHistory: irconfig.ValidatorsHistory{
						Height: map[uint32]uint32{
							0:  4,
							4:  1,
							12: 4,
						},
					},
					SetRolesInGenesis:               true,
					KeepOnlyLatestState:             true,
					RemoveUntraceableBlocks:         true,
					P2PNotaryRequestPayloadPoolSize: 100,
				},
			},
		}, cfg)
	})

	t.Run("incomplete", func(t *testing.T) {
		tests := []struct {
			name string
			yaml string
		}{
			{
				name: "magic",
				yaml: `
fschain:
  consensus:
    committee:
      - 02cddc58c3f7d27b5c9967dd90fbd4269798cbbb9cd7b137d886aca209cb734fb6
    storage:
      type: boltdb
      path: chain.db
`,
			},
			{
				name: "committee",
				yaml: `
fschain:
  consensus:
    magic: 15405
    storage:
      type: boltdb
      path: chain.db
`,
			},
			{
				name: "storage",
				yaml: `
fschain:
  consensus:
    magic: 15405
    committee:
      - 02cddc58c3f7d27b5c9967dd90fbd4269798cbbb9cd7b137d886aca209cb734fb6
`,
			},
			{
				name: "storage.type",
				yaml: `
fschain:
  consensus:
    magic: 15405
    committee:
      - 02cddc58c3f7d27b5c9967dd90fbd4269798cbbb9cd7b137d886aca209cb734fb6
    storage:
      path: chain.db
`,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := _newConfigFromYAML(t, tt.yaml, "")
				err := validateBlockchainConfig(cfg)
				t.Log(err)
				require.Error(t, err, tt.name)
			})
		}
	})

	t.Run("invalid", func(t *testing.T) {
		t.Run("without consensus", func(t *testing.T) {
			cfg := newValidBlockchainConfig(t, fullConfig)
			resetConfig(t, &cfg.FSChain.Consensus)
			err := validateBlockchainConfig(cfg)
			require.Error(t, err)
		})

		type kv struct {
			key string
			val any
		}

		kvF := func(k string, v any) kv {
			return kv{k, v}
		}

		processViper := func(t *testing.T, testCase []kv) (*viper.Viper, []string) {
			var reportMsg []string

			v := viper.New()
			v.SetConfigType("yaml")

			err := v.ReadConfig(strings.NewReader(validBlockchainConfigMinimal + validBlockchainConfigOptions))
			require.NoError(t, err)

			for _, kvPair := range testCase {
				key := kvPair.key
				val := kvPair.val

				v.Set("fschain.consensus."+key, val)
				reportMsg = append(reportMsg, fmt.Sprintf("%s=%v", key, val))
			}

			return v, reportMsg
		}

		t.Run("invalid unmarshaling", func(t *testing.T) {
			for _, testCase := range [][]kv{
				{kvF("magic", "not an integer")},
				{kvF("magic", -1)},
				{kvF("magic", 0.1)},
				{kvF("magic", math.MaxUint32+1)},
				{kvF("committee", []string{"not a key"})},
				{kvF("time_per_block", "not a duration")},
				{kvF("max_traceable_blocks", -1)},
				{kvF("max_traceable_blocks", math.MaxUint32+1)},
				{kvF("max_valid_until_block_increment", -1)},
				{kvF("max_valid_until_block_increment", math.MaxUint32+1)},
				{kvF("hardforks", "not a dictionary")},
				{kvF("hardforks", map[string]any{"name": "not a number"})},
				{kvF("hardforks", map[string]any{"name": -1})},
				{kvF("hardforks", map[string]any{"name": math.MaxUint32 + 1})},
				{kvF("validators_history", map[string]any{"not a number": 1})},
				{kvF("validators_history", map[string]any{"0": "not a number"})},
				{kvF("validators_history", map[string]any{"-1": 1})},
				{kvF("validators_history", map[string]any{"0": -1})},
				{kvF("validators_history", map[string]any{"0": math.MaxUint32 + 1})},
				{kvF("rpc.max_websocket_clients", -1)},
				{kvF("rpc.session_pool_size", -1)},
				{kvF("rpc.max_gas_invoke", -1)},
				{kvF("p2p.dial_timeout", "not a duration")},
				{kvF("p2p.proto_tick_interval", "not a duration")},
				{kvF("p2p.ping.interval", "not a duration")},
				{kvF("p2p.ping.timeout", "not a duration")},
				{kvF("p2p.peers.min", -1)},
				{kvF("p2p.peers.max", -1)},
				{kvF("p2p.peers.attempts", -1)},
				{kvF("set_roles_in_genesis", "not a boolean")},
				{kvF("set_roles_in_genesis", "not a boolean")},
				{kvF("p2p_notary_request_payload_pool_size", -1)},
				{kvF("p2p_notary_request_payload_pool_size", math.MaxUint32+1)},
			} {
				v, reportMsg := processViper(t, testCase)
				var cfg irconfig.Config
				err = configutil.Unmarshal(v, &cfg, irEnvPrefix)
				require.Error(t, err, strings.Join(reportMsg, ", "))
			}
		})

		t.Run("invalid validation", func(t *testing.T) {
			for _, testCase := range [][]kv{
				{kvF("magic", 0)},
				{kvF("committee", []string{})},
				{kvF("storage.type", "random string")},
				{kvF("time_per_block", -time.Second)},
				{kvF("seed_nodes", []string{"not a TCP address"})},
				{kvF("hardforks", map[string]any{"": 1})},
				{kvF("validators_history", map[string]any{"0": len(validCommittee) + 1})},
				{kvF("validators_history", map[string]any{"0": 1, "3": 1})}, // height is not a multiple
				{kvF("rpc.listen", []string{"not a TCP address"})},
				{kvF("rpc.tls.enabled", true), kvF("rpc.tls.cert_file", "")},                                       // enabled but no cert file is provided
				{kvF("rpc.tls.enabled", true), kvF("rpc.tls.cert_file", " \t")},                                    // enabled but no but blank cert is provided
				{kvF("rpc.tls.enabled", true), kvF("rpc.tls.cert_file", "/path/"), kvF("rpc.tls.key_file", "")},    // enabled but no key is provided
				{kvF("rpc.tls.enabled", true), kvF("rpc.tls.cert_file", "/path/"), kvF("rpc.tls.key_file", " \t")}, // enabled but no but blank key is provided
				{kvF("p2p.listen", []string{"not a TCP address"})},
				{kvF("p2p.dial_timeout", -time.Second)},
				{kvF("p2p.proto_tick_interval", -time.Second)},
				{kvF("p2p.ping.interval", -time.Second)},
				{kvF("p2p.ping.timeout", -time.Second)},
				{kvF("rpc.max_websocket_clients", math.MaxInt32+1)},
				{kvF("rpc.session_pool_size", math.MaxInt32+1)},
				{kvF("rpc.max_gas_invoke", math.MaxInt32+1)},
				{kvF("p2p.peers.min", math.MaxInt32+1)},
				{kvF("p2p.peers.max", math.MaxInt32+1)},
				{kvF("p2p.peers.attempts", math.MaxInt32+1)},
			} {
				v, reportMsg := processViper(t, testCase)
				cfg := unmarshalConfig(t, v)

				err = validateBlockchainConfig(cfg)
				require.Error(t, err, strings.Join(reportMsg, ", "))
			}
		})
	})

	t.Run("enums", func(t *testing.T) {
		t.Run("storage", func(t *testing.T) {
			cfg := newValidBlockchainConfig(t, fullConfig)
			const path = "path/to/db"

			cfg.FSChain.Consensus.Storage.Path = path
			cfg.FSChain.Consensus.Storage.Type = "boltdb"
			err := validateBlockchainConfig(cfg)
			require.NoError(t, err)

			resetConfig(t, &cfg.FSChain.Consensus.Storage.Path)
			cfg.Unset("fschain.consensus.storage.path")
			err = validateBlockchainConfig(cfg)
			require.Error(t, err)

			cfg.FSChain.Consensus.Storage.Path = path
			cfg.FSChain.Consensus.Storage.Type = "leveldb"
			err = validateBlockchainConfig(cfg)
			require.NoError(t, err)

			resetConfig(t, &cfg.FSChain.Consensus.Storage.Path)
			cfg.Unset("fschain.consensus.storage.path")
			err = validateBlockchainConfig(cfg)
			require.Error(t, err)

			// no path needed
			cfg.FSChain.Consensus.Storage.Type = "inmemory"
			err = validateBlockchainConfig(cfg)
			require.NoError(t, err)
		})
	})
}

func TestIsLocalConsensusMode(t *testing.T) {
	t.Run("ENV", func(t *testing.T) {
		const envKeyEndpoints = "NEOFS_IR_FSCHAIN_ENDPOINTS"
		const envKeyConsensus = "NEOFS_IR_FSCHAIN_CONSENSUS_MAGIC"
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
			v := viper.New()
			v.AutomaticEnv()
			v.SetEnvPrefix(irEnvPrefix)
			v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

			if tc.setEndpoints {
				err = os.Setenv(envKeyEndpoints, "any")
			} else {
				err = os.Unsetenv(envKeyEndpoints)
			}
			require.NoError(t, err, tc)

			if tc.setConsensus {
				err = os.Setenv(envKeyConsensus, "123")
			} else {
				err = os.Unsetenv(envKeyConsensus)
			}
			require.NoError(t, err, tc)

			cfg := unmarshalConfig(t, v)

			res, err := isLocalConsensusMode(cfg)
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

		cfg := unmarshalConfig(t, v)

		res, err := isLocalConsensusMode(cfg)
		require.NoError(t, err)
		require.False(t, res)

		resetConfig(t, &cfg.FSChain.Endpoints)

		_, err = isLocalConsensusMode(cfg)
		require.Error(t, err)

		cfg.FSChain.Consensus = irconfig.Consensus{}
		cfg.Set("fschain.consensus")

		res, err = isLocalConsensusMode(cfg)
		require.NoError(t, err)
		require.True(t, res)
	})
}

func TestP2PMinPeers(t *testing.T) {
	assert := func(t testing.TB, cfg *irconfig.Config, exp uint) {
		err := validateBlockchainConfig(cfg)
		require.NoError(t, err)
		require.EqualValues(t, exp, cfg.FSChain.Consensus.P2P.Peers.Min)
	}

	cfg := newValidBlockchainConfig(t, true)

	cfg.FSChain.Consensus.P2P.Peers.Min = 123
	assert(t, cfg, 123)

	t.Run("explicit zero", func(t *testing.T) {
		cfg := newValidBlockchainConfig(t, false)
		cfg.FSChain.Consensus.P2P.Peers.Min = 0
		cfg.Set("fschain.consensus.p2p.peers.min")
		assert(t, cfg, 0)
	})
	t.Run("default", func(t *testing.T) {
		assertDefault := func(t testing.TB, cfg *irconfig.Config) {
			setCommitteeN := func(n int) {
				cfg.FSChain.Consensus.Committee = commiteeN(t, n)
				resetConfig(t, &cfg.FSChain.Consensus.ValidatorsHistory) // checked against committee size
			}
			setCommitteeN(4)
			assert(t, cfg, 2)
			setCommitteeN(7)
			assert(t, cfg, 4)
			setCommitteeN(21)
			assert(t, cfg, 14)
		}
		t.Run("missing P2P section", func(t *testing.T) {
			cfg := newValidBlockchainConfig(t, true)
			resetConfig(t, &cfg.FSChain.Consensus.P2P)
			cfg.Unset("fschain.consensus.p2p")
			assertDefault(t, cfg)
		})
		t.Run("missing peers section", func(t *testing.T) {
			cfg := newValidBlockchainConfig(t, true)
			resetConfig(t, &cfg.FSChain.Consensus.P2P.Peers)
			cfg.Unset("fschain.consensus.p2p.peers")
			require.True(t, cfg.IsSet("fschain.consensus.p2p"))
			assertDefault(t, cfg)
		})
		t.Run("missing config itself", func(t *testing.T) {
			cfg := newValidBlockchainConfig(t, true)
			resetConfig(t, &cfg.FSChain.Consensus.P2P.Peers.Min)
			cfg.Unset("fschain.consensus.p2p.peers.min")
			require.True(t, cfg.IsSet("fschain.consensus.p2p.peers"))
			assertDefault(t, cfg)
		})
	})
}

func TestEnvOverride(t *testing.T) {
	const envKeyMagic = "NEOFS_IR_FSCHAIN_CONSENSUS_MAGIC"
	const magic = 123
	require.NoError(t, os.Setenv(envKeyMagic, strconv.Itoa(magic)))
	cfg := newValidBlockchainConfig(t, false)

	require.Equal(t, cfg.FSChain.Consensus.Magic, uint32(magic))
}
