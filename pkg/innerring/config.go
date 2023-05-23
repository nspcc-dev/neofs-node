package innerring

import (
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

// checks if Inner Ring app is configured to be launched in local consensus
// mode.
func isLocalConsensusMode(cfg *viper.Viper) bool {
	const morphRPCSectionDeprecated = "morph.endpoint.client"
	// first expression required for ENVs in which nesting breaks
	deprecatedNotSet := !cfg.IsSet(morphRPCSectionDeprecated+".0.address") && !cfg.IsSet(morphRPCSectionDeprecated)
	actualNotSet := !cfg.IsSet("morph.endpoints")

	return deprecatedNotSet && actualNotSet
}

func parseBlockchainConfig(v *viper.Viper, _logger *logger.Logger) (c blockchain.Config, err error) {
	const rootSection = "morph.consensus"

	if !v.IsSet(rootSection) {
		return c, fmt.Errorf("missing root section '%s'", rootSection)
	}

	_uint, err := parseConfigUint64Range(v, rootSection+".magic", "network magic", 1, math.MaxUint32)
	if err != nil {
		return c, err
	}
	c.NetworkMagic = netmode.Magic(_uint)

	const storageSection = rootSection + ".storage"
	if !v.IsSet(storageSection) {
		return c, fmt.Errorf("missing storage section '%s'", storageSection)
	}
	const storageTypeKey = storageSection + ".type"
	if !v.IsSet(storageTypeKey) {
		return c, fmt.Errorf("missing storage type '%s'", storageTypeKey)
	}
	const storagePathKey = storageSection + ".path"
	switch typ := v.GetString(storageTypeKey); typ {
	default:
		return c, fmt.Errorf("unsupported storage type '%s': '%s'", storageTypeKey, typ)
	case dbconfig.BoltDB:
		if !v.IsSet(storagePathKey) {
			return c, fmt.Errorf("missing path to the BoltDB '%s'", storagePathKey)
		}
		c.Storage = blockchain.BoltDB(v.GetString(storagePathKey))
	case dbconfig.LevelDB:
		if !v.IsSet(storagePathKey) {
			return c, fmt.Errorf("missing path to the LevelDB '%s'", storagePathKey)
		}
		c.Storage = blockchain.LevelDB(v.GetString(storagePathKey))
	case dbconfig.InMemoryDB:
		c.Storage = blockchain.InMemory()
	}

	const committeeKey = rootSection + ".committee"
	c.Committee, err = parseConfigPublicKeys(v, committeeKey, "committee members")
	if err != nil {
		return c, err
	} else if len(c.Committee) == 0 {
		return c, fmt.Errorf("empty committee members '%s'", committeeKey)
	}

	c.BlockInterval, err = parseConfigDurationPositive(v, rootSection+".time_per_block", "block interval")
	if err != nil && !errors.Is(err, errMissingConfig) {
		return c, err
	}

	traceableChainLength, err := parseConfigUint64Range(v, rootSection+".max_traceable_blocks", "traceable chain length", 1, math.MaxUint32)
	if err != nil && !errors.Is(err, errMissingConfig) {
		return c, err
	}
	c.TraceableChainLength = uint32(traceableChainLength)

	c.SeedNodes, err = parseConfigAddressesTCP(v, rootSection+".seed_nodes", "seed nodes")
	if err != nil && !errors.Is(err, errMissingConfig) {
		return c, err
	}

	const hardForksKey = rootSection + ".hardforks"
	if v.IsSet(hardForksKey) {
		c.HardForks, err = parseConfigMapUint32(v, hardForksKey, "hard forks", math.MaxUint32)
		if err != nil {
			return c, err
		}
	}

	const validatorsHistoryKey = rootSection + ".validators_history"
	if v.IsSet(validatorsHistoryKey) {
		c.ValidatorsHistory = make(map[uint32]uint32)
		err = parseConfigMap(v, validatorsHistoryKey, "validators history", func(name string, val interface{}) error {
			height, err := strconv.ParseUint(name, 10, 32)
			if err != nil {
				return fmt.Errorf("parse unsigned integer: %w", err)
			}
			num, err := cast.ToUint32E(val)
			if err != nil {
				return err
			} else if num > math.MaxInt32 {
				return fmt.Errorf("value %d is out of allowable range", num)
			}
			c.ValidatorsHistory[uint32(height)] = num
			return nil
		})
		if err != nil {
			return c, err
		}
	}

	const nativeActivationsKey = rootSection + ".native_activations"
	if v.IsSet(nativeActivationsKey) {
		requiredContracts := []string{
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
		c.NativeActivations = make(map[string][]uint32)
		err = parseConfigMap(v, nativeActivationsKey, "native update histories", func(name string, val interface{}) error {
			supported := false
			for i := range requiredContracts {
				// TODO: viper lowers YAML keys, so, for example, 'NativeContract' becomes 'nativecontract'
				//  Track https://github.com/spf13/viper/issues/1014. Until then, keep track of
				//  new contracts in nativenames package.
				supported = name == strings.ToLower(requiredContracts[i])
				if supported {
					name = requiredContracts[i]
					break
				}
			}
			if !supported {
				return fmt.Errorf("unsupported Neo native contract name '%s'", name)
			}
			rawHeights, err := cast.ToSliceE(val)
			if err != nil {
				return fmt.Errorf("cast value to array '%s': %w", name, err)
			} else if len(rawHeights) == 0 {
				return fmt.Errorf("empty array '%s'", name)
			}
			heights := make([]uint32, len(rawHeights))
			for i := range rawHeights {
				u64, err := cast.ToUint64E(rawHeights[i])
				if err != nil {
					return fmt.Errorf("cast element to unsigned integer '%s' #%d: %w", name, i, err)
				} else if u64 > math.MaxUint32 {
					return fmt.Errorf("value overflows limit %v", u64)
				}
				heights[i] = uint32(u64)
			}
			c.NativeActivations[name] = heights
			return nil
		})
		if err != nil {
			return c, err
		}
		for i := range requiredContracts {
			if _, ok := c.NativeActivations[requiredContracts[i]]; !ok {
				return c, fmt.Errorf("missing required Neo native contract name '%s' in '%s'", requiredContracts[i], nativeActivationsKey)
			}
		}
	}

	const rpcSection = rootSection + ".rpc"
	if v.IsSet(rpcSection) {
		c.RPC.Addresses, err = parseConfigAddressesTCP(v, rpcSection+".listen", "network addresses to listen insecure Neo RPC on")
		if err != nil && !errors.Is(err, errMissingConfig) {
			return c, err
		}

		const rpcTLSSection = rpcSection + ".tls"
		if v.GetBool(rpcTLSSection + ".enabled") {
			c.RPC.TLSConfig.Enabled = true

			c.RPC.TLSConfig.Addresses, err = parseConfigAddressesTCP(v, rpcTLSSection+".listen", "network addresses to listen to Neo RPC over TLS")
			if err != nil {
				return c, err
			}

			const certCfgKey = rpcTLSSection + ".cert_file"
			c.RPC.TLSConfig.CertFile = v.GetString(certCfgKey)
			if strings.TrimSpace(c.RPC.TLSConfig.CertFile) == "" {
				return c, fmt.Errorf("RPC TLS setup is enabled but no certificate ('%s') is provided", certCfgKey)
			}

			const keyCfgKey = rpcTLSSection + ".key_file"
			c.RPC.TLSConfig.KeyFile = v.GetString(keyCfgKey)
			if strings.TrimSpace(c.RPC.TLSConfig.KeyFile) == "" {
				return c, fmt.Errorf("RPC TLS setup is enabled but no key ('%s') is provided", keyCfgKey)
			}
		}
	}

	const p2pSection = rootSection + ".p2p"
	if v.IsSet(p2pSection) {
		c.P2P.DialTimeout, err = parseConfigDurationPositive(v, p2pSection+".dial_timeout", "P2P dial timeout")
		if err != nil && !errors.Is(err, errMissingConfig) {
			return c, err
		}
		c.P2P.ProtoTickInterval, err = parseConfigDurationPositive(v, p2pSection+".proto_tick_interval", "P2P protocol tick interval")
		if err != nil && !errors.Is(err, errMissingConfig) {
			return c, err
		}
		c.P2P.ListenAddresses, err = parseConfigAddressesTCP(v, p2pSection+".listen", "network addresses to listen Neo P2P on")
		if err != nil && !errors.Is(err, errMissingConfig) {
			return c, err
		}
		const p2pPeersSection = p2pSection + ".peers"
		if v.IsSet(p2pPeersSection) {
			minPeers, err := parseConfigUint64Max(v, p2pPeersSection+".min", "minimum number of P2P peers", math.MaxInt32)
			if err != nil {
				if !errors.Is(err, errMissingConfig) {
					return c, err
				}
				// we calculate default here since explicit 0 is also a valid setting
				n := uint64(len(c.Committee))
				minPeers = n - (n-1)/3 - 1
			}
			c.P2P.MinPeers = uint(minPeers)
			maxPeers, err := parseConfigUint64Range(v, p2pPeersSection+".max", "maximum number of P2P peers", 1, math.MaxInt32)
			if err != nil && !errors.Is(err, errMissingConfig) {
				return c, err
			}
			c.P2P.MaxPeers = uint(maxPeers)
			attemptConnPeers, err := parseConfigUint64Range(v, p2pPeersSection+".attempts", "number of P2P connection attempts", 1, math.MaxInt32)
			if err != nil && !errors.Is(err, errMissingConfig) {
				return c, err
			}
			c.P2P.AttemptConnPeers = uint(attemptConnPeers)
		}
		const pingSection = p2pSection + ".ping"
		if v.IsSet(pingSection) {
			c.P2P.Ping.Interval, err = parseConfigDurationPositive(v, pingSection+".interval", "P2P ping interval")
			if err != nil {
				return c, err
			}
			c.P2P.Ping.Timeout, err = parseConfigDurationPositive(v, pingSection+".timeout", "P2P ping timeout")
			if err != nil {
				return c, err
			}
		}
	}

	c.Logger = _logger.Logger

	return c, nil
}

var errMissingConfig = errors.New("config value is missing")

func parseConfigUint64Condition(v *viper.Viper, key, desc string, cond func(uint64) error) (uint64, error) {
	var res uint64
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		res, err = cast.ToUint64E(v.Get(key))
		if err == nil && cond != nil {
			err = cond(res)
		}
	}
	if err != nil {
		return res, fmt.Errorf("invalid %s '%s' (unsigned integer): %w", desc, key, err)
	}
	return res, nil
}

func parseConfigUint64Range(v *viper.Viper, key, desc string, min, max uint64) (uint64, error) {
	return parseConfigUint64Condition(v, key, desc, func(val uint64) error {
		if val < min || val > max {
			return fmt.Errorf("out of allowable range [%d:%d]", min, max)
		}
		return nil
	})
}

func parseConfigUint64Max(v *viper.Viper, key, desc string, max uint64) (uint64, error) {
	return parseConfigUint64Range(v, key, desc, 0, max)
}

func parseConfigDurationCondition(v *viper.Viper, key, desc string, cond func(time.Duration) error) (time.Duration, error) {
	var res time.Duration
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		res, err = cast.ToDurationE(v.Get(key))
		if err == nil && cond != nil {
			err = cond(res)
		}
	}
	if err != nil {
		return res, fmt.Errorf("invalid %s '%s' (duration): %w", desc, key, err)
	}
	return res, nil
}

func parseConfigDurationPositive(v *viper.Viper, key, desc string) (time.Duration, error) {
	return parseConfigDurationCondition(v, key, desc, func(d time.Duration) error {
		if d <= 0 {
			return errors.New("must be positive")
		}
		return nil
	})
}

func parseConfigStrings(v *viper.Viper, key, desc string) ([]string, error) {
	var res []string
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		res, err = cast.ToStringSliceE(v.Get(key))
	}
	if err != nil {
		return res, fmt.Errorf("invalid %s '%s' (string array): %w", desc, key, err)
	}
	return res, nil
}

func parseConfigPublicKeys(v *viper.Viper, key, desc string) (keys.PublicKeys, error) {
	ss, err := parseConfigStrings(v, key, desc)
	if err != nil {
		return nil, err
	}
	res, err := keys.NewPublicKeysFromStrings(ss)
	if err != nil {
		return res, fmt.Errorf("invalid %s '%s' (public keys): %w", desc, key, err)
	}
	return res, nil
}

func parseConfigAddressesTCP(v *viper.Viper, key, desc string) ([]string, error) {
	ss, err := parseConfigStrings(v, key, desc)
	if err != nil {
		return nil, err
	}
	for i := range ss {
		_, err = net.ResolveTCPAddr("tcp", ss[i])
		if err != nil {
			return ss, fmt.Errorf("invalid %s '%s' (TCP addresses): %w", desc, key, err)
		}
	}
	return ss, nil
}

func parseConfigMap(v *viper.Viper, key, desc string, f func(name string, val interface{}) error) error {
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		var m map[string]interface{}
		m, err = cast.ToStringMapE(v.Get(key))
		if err == nil {
			for name, val := range m {
				err = f(name, val)
				if err != nil {
					err = fmt.Errorf("invalid element '%s': %w", name, err)
					break
				}
			}
		}
	}
	if err != nil {
		return fmt.Errorf("invalid %s '%s' (dictionary): %w", desc, key, err)
	}
	return nil
}

func parseConfigMapUint32(v *viper.Viper, key, desc string, limit uint64) (map[string]uint32, error) {
	res := make(map[string]uint32)
	return res, parseConfigMap(v, key, desc, func(name string, val interface{}) error {
		if name == "" {
			return errors.New("empty key")
		}
		u64, err := cast.ToUint64E(val)
		if err == nil {
			if u64 > limit {
				err = fmt.Errorf("value overflows limit %v", u64)
			} else {
				res[name] = uint32(u64)
			}
		}
		return err
	})
}
