package innerring

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Path of YAML configuration of the IR consensus with all required fields.
const validConfigMinimal = `
morph:
  consensus:
    magic: 15405
    committee:
      - 02cddc58c3f7d27b5c9967dd90fbd4269798cbbb9cd7b137d886aca209cb734fb6
      - 03f87b0a0416e4028bccf7258db3b411412ce1c7426b2c857f54e59d0d23782570
    storage:
      type: boltdb
      path: chain.db
`

// Path of YAML configuration of the IR consensus with all optional fields.
const validConfigOptions = `
    notary_disabled: true
    time_per_block: 1s
    max_traceable_blocks: 200
    seed_nodes:
      - localhost:20000
      - localhost:20001
    hardforks:
      name: 1730000
    validators_history:
      2: 3
      10: 7
    native_activations:
      ContractManagement: [0, 1]
      LedgerContract: [2]
      NeoToken: [3]
      GasToken: [4]
      PolicyContract: [5]
      OracleContract: [6]
      RoleManagement: [7]
      Notary: [8]
      CryptoLib: [9]
      StdLib: [10]
    rpc:
      listen:
        - localhost:30000
        - localhost:30001
    p2p:
      dial_timeout: 111s
      proto_tick_interval: 222s
      listen:
        - localhost:20100
        - localhost:20101
      peers:
        min: 1
        max: 5
        attempts: 20 # How many peers node should try to dial after falling under 'min' count. Must be in range [1:2147483647]
      ping:
        interval: 44s
        timeout: 55s
`

// returns viper.Viper initialized from valid configuration above.
func newValidConfig(tb testing.TB, full bool) *viper.Viper {
	v := viper.New()
	v.SetConfigType("yaml")

	src := validConfigMinimal
	if full {
		src += validConfigOptions
	}

	err := v.ReadConfig(strings.NewReader(src))
	require.NoError(tb, err)

	return v
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
			m, ok := v.(map[string]interface{})
			require.Truef(tb, ok, "unsupported type: %T for %q", v, strings.Join(parts[0:i], "."))
			mAllCp = m
		}
	}

	*v = *viper.New()

	for key, val := range mAll {
		v.Set(key, val)
	}
}

func TestConfigParser(t *testing.T) {
	fullConfig := true
	_logger := &logger.Logger{Logger: zap.NewNop()}

	validCommittee, err := keys.NewPublicKeysFromStrings([]string{
		"02cddc58c3f7d27b5c9967dd90fbd4269798cbbb9cd7b137d886aca209cb734fb6",
		"03f87b0a0416e4028bccf7258db3b411412ce1c7426b2c857f54e59d0d23782570",
	})
	require.NoError(t, err)

	t.Run("minimal", func(t *testing.T) {
		v := newValidConfig(t, !fullConfig)
		c, err := parseBlockchainConfig(v, _logger)
		require.NoError(t, err)

		require.Equal(t, blockchain.Config{
			Logger:       _logger.Logger,
			NetworkMagic: 15405,
			Committee:    validCommittee,
			Storage:      blockchain.BoltDB("chain.db"),
		}, c)
	})

	t.Run("full", func(t *testing.T) {
		v := newValidConfig(t, fullConfig)
		c, err := parseBlockchainConfig(v, _logger)
		require.NoError(t, err)

		require.Equal(t, blockchain.Config{
			Logger:        _logger.Logger,
			NetworkMagic:  15405,
			Committee:     validCommittee,
			BlockInterval: time.Second,
			RPCListenAddresses: []string{
				"localhost:30000",
				"localhost:30001",
			},
			TraceableChainLength: 200,
			HardForks: map[string]uint32{
				"name": 1730000,
			},
			SeedNodes: []string{
				"localhost:20000",
				"localhost:20001",
			},
			NotaryDisabled: true,
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
				},
			},
			Storage: blockchain.BoltDB("chain.db"),
			ValidatorsHistory: map[uint32]uint32{
				2:  3,
				10: 7,
			},
			NativeActivations: map[string][]uint32{
				nativenames.Management:  {0, 1},
				nativenames.Ledger:      {2},
				nativenames.Neo:         {3},
				nativenames.Gas:         {4},
				nativenames.Policy:      {5},
				nativenames.Oracle:      {6},
				nativenames.Designation: {7},
				nativenames.Notary:      {8},
				nativenames.CryptoLib:   {9},
				nativenames.StdLib:      {10},
			},
		}, c)
	})

	t.Run("incomplete", func(t *testing.T) {
		for _, requiredKey := range []string{
			"magic",
			"committee",
			"storage",
			"storage.type",
		} {
			v := newValidConfig(t, !fullConfig)
			resetConfig(t, v, "morph.consensus."+requiredKey)
			_, err := parseBlockchainConfig(v, _logger)
			require.Error(t, err, requiredKey)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		v := newValidConfig(t, fullConfig)
		resetConfig(t, v, "morph.consensus")
		_, err := parseBlockchainConfig(v, _logger)
		require.Error(t, err)

		for _, testCase := range []struct {
			key string
			val interface{}
		}{
			{"magic", "not an integer"},
			{"magic", -1},
			{"magic", 0},
			{"magic", math.MaxUint32 + 1},
			{"committee", []string{}},
			{"committee", []string{"not a key"}},
			{"storage.type", "random string"},
			{"notary_disabled", "not a boolean"},
			{"time_per_block", "not a duration"},
			{"time_per_block", -time.Second},
			{"max_traceable_blocks", -1},
			{"max_traceable_blocks", math.MaxUint32 + 1},
			{"seed_nodes", []string{"not a TCP address"}},
			{"seed_nodes", []string{"127.0.0.1"}}, // missing port
			{"hardforks", "not a dictionary"},
			{"hardforks", map[string]interface{}{"": 1}},
			{"hardforks", map[string]interface{}{"name": "not a number"}},
			{"hardforks", map[string]interface{}{"name": -1}},
			{"hardforks", map[string]interface{}{"name": math.MaxUint32 + 1}},
			{"validators_history", map[string]interface{}{"not a number": 1}},
			{"validators_history", map[string]interface{}{"1": "not a number"}},
			{"validators_history", map[string]interface{}{"-1": 1}},
			{"validators_history", map[string]interface{}{"1": -1}},
			{"validators_history", map[string]interface{}{"1": math.MaxInt32 + 1}},
			{"native_activations", map[string]interface{}{"1": ""}},
			{"native_activations", map[string]interface{}{strings.ToLower(nativenames.Gas): "not an array"}},
			{"native_activations", map[string]interface{}{strings.ToLower(nativenames.Gas): []interface{}{}}},
			{"native_activations", map[string]interface{}{strings.ToLower(nativenames.Gas): []interface{}{"not a number"}}},
			{"native_activations", map[string]interface{}{strings.ToLower(nativenames.Gas): []interface{}{-1}}},
			{"native_activations", map[string]interface{}{strings.ToLower(nativenames.Gas): []interface{}{math.MaxUint32 + 1}}},
			{"rpc.listen", []string{"not a TCP address"}},
			{"rpc.listen", []string{"127.0.0.1"}}, // missing port
			{"p2p.listen", []string{"not a TCP address"}},
			{"p2p.listen", []string{"127.0.0.1"}}, // missing port
			{"p2p.dial_timeout", "not a duration"},
			{"p2p.dial_timeout", -time.Second},
			{"p2p.proto_tick_interval", "not a duration"},
			{"p2p.proto_tick_interval", -time.Second},
			{"p2p.ping.interval", "not a duration"},
			{"p2p.ping.interval", -time.Second},
			{"p2p.ping.timeout", "not a duration"},
			{"p2p.ping.timeout", -time.Second},
			{"p2p.peers.min", -1},
			{"p2p.peers.min", math.MaxInt32 + 1},
			{"p2p.peers.max", -1},
			{"p2p.peers.max", math.MaxInt32 + 1},
			{"p2p.peers.attempts", -1},
			{"p2p.peers.attempts", math.MaxInt32 + 1},
		} {
			v := newValidConfig(t, fullConfig)
			v.Set("morph.consensus."+testCase.key, testCase.val)
			_, err := parseBlockchainConfig(v, _logger)
			require.Errorf(t, err, "%s=%v", testCase.key, testCase.val)
		}
	})

	t.Run("enums", func(t *testing.T) {
		t.Run("storage", func(t *testing.T) {
			v := newValidConfig(t, fullConfig)
			const path = "path/to/db"

			v.Set("morph.consensus.storage.path", path)
			v.Set("morph.consensus.storage.type", "boltdb")
			c, err := parseBlockchainConfig(v, _logger)
			require.NoError(t, err)
			require.Equal(t, blockchain.BoltDB(path), c.Storage)

			resetConfig(t, v, "morph.consensus.storage.path")
			_, err = parseBlockchainConfig(v, _logger)
			require.Error(t, err)

			v.Set("morph.consensus.storage.path", path)
			v.Set("morph.consensus.storage.type", "leveldb")
			c, err = parseBlockchainConfig(v, _logger)
			require.NoError(t, err)
			require.Equal(t, blockchain.LevelDB(path), c.Storage)

			resetConfig(t, v, "morph.consensus.storage.path")
			_, err = parseBlockchainConfig(v, _logger)
			require.Error(t, err)

			// no path needed
			v.Set("morph.consensus.storage.type", "inmemory")
			c, err = parseBlockchainConfig(v, _logger)
			require.NoError(t, err)
			require.Equal(t, blockchain.InMemory(), c.Storage)
		})

		t.Run("native activations", func(t *testing.T) {
			names := []string{
				nativenames.Management,
				nativenames.Ledger,
				nativenames.Neo,
				nativenames.Gas,
				nativenames.Policy,
				nativenames.Oracle,
				nativenames.Designation,
				nativenames.Notary,
				nativenames.CryptoLib,
				nativenames.StdLib,
			}

			v := newValidConfig(t, fullConfig)

			setI := func(name string, i int) {
				v.Set("morph.consensus.native_activations."+strings.ToLower(name), []interface{}{i})
			}

			for i := range names {
				setI(names[i], i)
			}

			c, err := parseBlockchainConfig(v, _logger)
			require.NoError(t, err)

			for i := range names {
				v, ok := c.NativeActivations[names[i]]
				require.True(t, ok, names[i])
				require.ElementsMatch(t, []uint32{uint32(i)}, v, names[i])
			}

			resetConfig(t, v, "morph.consensus.native_activations."+strings.ToLower(names[0]))
			_, err = parseBlockchainConfig(v, _logger)
			require.Error(t, err)

			setI(names[0], 0)
			badName := names[0] + "1" // almost definitely incorrect
			setI(badName, 0)
			_, err = parseBlockchainConfig(v, _logger)
			require.Error(t, err)
		})
	})
}
