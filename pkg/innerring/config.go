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
	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// checks whether Inner Ring app is configured to initialize underlying NeoFS
// Sidechain or await for a background deployment.
func isAutoDeploymentMode(cfg *viper.Viper) bool {
	return cfg.IsSet("network_settings")
}

// checks if Inner Ring app is configured to be launched in local consensus
// mode.
func isLocalConsensusMode(cfg *viper.Viper) bool {
	return !cfg.IsSet("morph.endpoints")
}

func parseBlockchainConfig(v *viper.Viper, _logger *zap.Logger) (c blockchain.Config, err error) {
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

	c.Logger = _logger

	return c, nil
}

const networkSettingsConfigSection = "network_settings"

func parseNetworkSettingsConfig(v *viper.Viper) (c netmap.NetworkConfiguration, err error) {
	if !v.IsSet(networkSettingsConfigSection) {
		return c, fmt.Errorf("missing root section '%s'", networkSettingsConfigSection)
	}

	c.EpochDuration, err = parseConfigUint64Range(v, networkSettingsConfigSection+".epoch_duration", "epoch duration", 1, math.MaxUint32)
	if err != nil {
		return
	}

	c.MaxObjectSize, err = parseConfigUint64Range(v, networkSettingsConfigSection+".max_object_size", "max object size", 1, math.MaxUint64)
	if err != nil {
		return
	}

	requireHomoHash, err := parseConfigBool(v, networkSettingsConfigSection+".require_homomorphic_hashing", "is homomorphic hashing required")
	if err != nil {
		return
	}

	c.HomomorphicHashingDisabled = !requireHomoHash

	c.MaintenanceModeAllowed, err = parseConfigBool(v, networkSettingsConfigSection+".allow_maintenance_mode", "is maintenance mode allowed")
	if err != nil {
		return
	}

	const eigenTrustSection = networkSettingsConfigSection + ".eigen_trust"
	if !v.IsSet(eigenTrustSection) {
		return c, fmt.Errorf("missing EigenTrust section '%s'", eigenTrustSection)
	}

	c.EigenTrustAlpha, err = parseConfigFloatRange(v, eigenTrustSection+".alpha", "EigenTrust alpha parameter", 0, 1)
	if err != nil {
		return
	}

	c.EigenTrustIterations, err = parseConfigUint64Range(v, eigenTrustSection+".iterations_number", "number of EigenTrust iterations", 1, math.MaxUint64)
	if err != nil {
		return
	}

	const priceSection = networkSettingsConfigSection + ".price"
	if !v.IsSet(priceSection) {
		return c, fmt.Errorf("missing price section '%s'", priceSection)
	}

	c.StoragePrice, err = parseConfigUint64Max(v, priceSection+".storage", "storage price", math.MaxUint64)
	if err != nil {
		return
	}

	const feeSection = priceSection + ".fee"
	if !v.IsSet(feeSection) {
		return c, fmt.Errorf("missing fee section '%s'", feeSection)
	}

	c.IRCandidateFee, err = parseConfigUint64Max(v, feeSection+".ir_candidate", "Inner Ring candidate fee", math.MaxUint64)
	if err != nil {
		return
	}

	c.WithdrawalFee, err = parseConfigUint64Max(v, feeSection+".withdraw", "withdrawal fee", math.MaxUint64)
	if err != nil {
		return
	}

	c.AuditFee, err = parseConfigUint64Max(v, feeSection+".audit", "data audit fee", math.MaxUint64)
	if err != nil {
		return
	}

	c.ContainerFee, err = parseConfigUint64Max(v, feeSection+".new_container", "container creation fee", math.MaxUint64)
	if err != nil {
		return
	}

	c.ContainerAliasFee, err = parseConfigUint64Max(v, feeSection+".container_domain", "container domain fee", math.MaxUint64)
	if err != nil {
		return
	}

	customSettingsKey := networkSettingsConfigSection + ".custom"
	if v.IsSet(customSettingsKey) {
		var sss []string
		sss, err = parseConfigStrings(v, customSettingsKey, "custom settings")
		if err != nil {
			return
		}

		if len(sss) == 0 {
			return c, fmt.Errorf("missing custom settings '%s'", customSettingsKey)
		}

		c.Raw = make([]netmap.RawNetworkParameter, len(sss))

		for i := range sss {
			const sep = "="
			ss := strings.Split(sss[i], sep)
			if len(ss) != 2 {
				return c, fmt.Errorf("invalid %s '%s' (%s-separated key-value): failed to parse element #%d", customSettingsKey, ss[i], sep, i)
			}

			switch ss[0] {
			default:
				for j := 0; j < i; j++ {
					if ss[0] == c.Raw[j].Name {
						return c, fmt.Errorf("duplicated custom network setting '%s' in '%s'", ss[0], customSettingsKey)
					}
				}
			case "AuditFee",
				"BasicIncomeRate",
				"ContainerAliasFee",
				"ContainerFee",
				"EigenTrustAlpha",
				"EigenTrustIterations",
				"EpochDuration",
				"HomomorphicHashingDisabled",
				"InnerRingCandidateFee",
				"MaintenanceModeAllowed",
				"MaxObjectSize",
				"WithdrawFee":
				return c, fmt.Errorf("invalid %s '%s' (%s-separated key-value): key to element #%d is forbidden", customSettingsKey, ss[i], sep, i)
			}

			c.Raw[i].Name = ss[0]
			c.Raw[i].Value = []byte(ss[1])
		}
	}

	return
}

type nnsConfig struct {
	systemEmail string
}

func parseNNSConfig(v *viper.Viper) (c nnsConfig, err error) {
	const rootSection = "nns"

	if !v.IsSet(rootSection) {
		return c, fmt.Errorf("missing root section '%s'", rootSection)
	}

	c.systemEmail, err = parseConfigString(v, rootSection+".system_email", "system email for NNS")

	return
}

var errMissingConfig = errors.New("config value is missing")

func parseConfigUint64Condition(v *viper.Viper, key, desc string, cond func(uint64) error) (uint64, error) {
	var res uint64
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		switch val := v.Get(key).(type) {
		case float32, float64:
			// cast.ToUint64E just drops mantissa
			return 0, fmt.Errorf("unable to cast %#v of type %T to uint64", val, val)
		default:
			res, err = cast.ToUint64E(val)
		}
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

func parseConfigBool(v *viper.Viper, key, desc string) (bool, error) {
	var res bool
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		switch val := v.GetString(key); val {
		default:
			err = errors.New("neither true nor false")
		case "false":
		case "true":
			res = true
		}
	}
	if err != nil {
		return res, fmt.Errorf("invalid %s '%s' (boolean): %w", desc, key, err)
	}
	return res, nil
}

func parseConfigFloatRange(v *viper.Viper, key, desc string, min, max float64) (float64, error) {
	var res float64
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		res, err = cast.ToFloat64E(v.Get(key))
		if err == nil {
			if res < min || res > max {
				err = fmt.Errorf("out of allowable range [%.2f:%.2f]", min, max)
			}
		}
	}
	if err != nil {
		return res, fmt.Errorf("invalid %s '%s' (boolean): %w", desc, key, err)
	}
	return res, nil
}

func parseConfigString(v *viper.Viper, key, desc string) (string, error) {
	var res string
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		res, err = cast.ToStringE(v.Get(key))
		if err == nil && res == "" {
			err = errMissingConfig
		}
	}
	if err != nil {
		return res, fmt.Errorf("invalid %s '%s' (string): %w", desc, key, err)
	}
	return res, nil
}
