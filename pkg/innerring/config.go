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
	"github.com/nspcc-dev/neofs-contract/deploy"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// Various configuration paths.
const (
	cfgPathFSChain               = "morph"
	cfgPathFSChainRPCEndpoints   = cfgPathFSChain + ".endpoints"
	cfgPathFSChainLocalConsensus = cfgPathFSChain + ".consensus"
	cfgPathFSChainValidators     = cfgPathFSChain + ".validators"
)

// Default ports for listening on TCP addresses.
const (
	p2pDefaultListenPort = "20333"
	rpcDefaultListenPort = "30333"
)

// checks whether Inner Ring app is configured to initialize underlying FS chain
// or await for a background deployment. Returns an error if
// the configuration format is violated.
func isAutoDeploymentMode(cfg *viper.Viper) (bool, error) {
	res, err := parseConfigBool(cfg, "fschain_autodeploy", "flag to auto-deploy the FS chain")
	if err != nil && !errors.Is(err, errMissingConfig) {
		return false, err
	}

	return res, nil
}

// checks if Inner Ring app is configured to be launched in local consensus
// mode. Returns error if neither NeoFS chain RPC endpoints nor local consensus
// is configured.
func isLocalConsensusMode(cfg *viper.Viper) (bool, error) {
	endpointsUnset := !cfg.IsSet(cfgPathFSChainRPCEndpoints)
	if endpointsUnset && !cfg.IsSet(cfgPathFSChainLocalConsensus) {
		return false, fmt.Errorf("either '%s' or '%s' must be configured",
			cfgPathFSChainRPCEndpoints, cfgPathFSChainLocalConsensus)
	}

	return endpointsUnset, nil
}

func parseBlockchainConfig(v *viper.Viper, _logger *zap.Logger) (c blockchain.Config, err error) {
	if !v.IsSet(cfgPathFSChainLocalConsensus) {
		return c, fmt.Errorf("missing root section '%s'", cfgPathFSChainLocalConsensus)
	}

	_uint, err := parseConfigUint64Range(v, cfgPathFSChainLocalConsensus+".magic", "network magic", 1, math.MaxUint32)
	if err != nil {
		return c, err
	}
	c.NetworkMagic = netmode.Magic(_uint)

	const storageSection = cfgPathFSChainLocalConsensus + ".storage"
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

	const committeeKey = cfgPathFSChainLocalConsensus + ".committee"
	c.Committee, err = parseConfigPublicKeys(v, committeeKey, "committee members")
	if err != nil {
		return c, err
	} else if len(c.Committee) == 0 {
		return c, fmt.Errorf("empty committee members '%s'", committeeKey)
	}

	c.BlockInterval, err = parseConfigDurationPositive(v, cfgPathFSChainLocalConsensus+".time_per_block", "block interval")
	if err != nil && !errors.Is(err, errMissingConfig) {
		return c, err
	}

	traceableChainLength, err := parseConfigUint64Range(v, cfgPathFSChainLocalConsensus+".max_traceable_blocks", "traceable chain length", 1, math.MaxUint32)
	if err != nil && !errors.Is(err, errMissingConfig) {
		return c, err
	}
	c.TraceableChainLength = uint32(traceableChainLength)

	c.SeedNodes, err = parseConfigAddressesTCP(v, cfgPathFSChainLocalConsensus+".seed_nodes", "seed nodes", p2pDefaultListenPort)
	if err != nil && !errors.Is(err, errMissingConfig) {
		return c, err
	}

	const hardForksKey = cfgPathFSChainLocalConsensus + ".hardforks"
	if v.IsSet(hardForksKey) {
		c.HardForks, err = parseConfigMapUint32(v, hardForksKey, "hard forks", math.MaxUint32)
		if err != nil {
			return c, err
		}
	}

	const validatorsHistoryKey = cfgPathFSChainLocalConsensus + ".validators_history"
	if v.IsSet(validatorsHistoryKey) {
		c.ValidatorsHistory = make(map[uint32]uint32)
		committeeSize := uint64(c.Committee.Len())
		err = parseConfigMap(v, validatorsHistoryKey, "validators history", func(name string, val any) error {
			height, err := strconv.ParseUint(name, 10, 32)
			if err != nil {
				return fmt.Errorf("parse unsigned integer: %w", err)
			}

			if height%committeeSize != 0 {
				return fmt.Errorf("height %d is not a multiple of the %q size", height, committeeKey)
			}

			num, err := cast.ToUint32E(val)
			if err != nil {
				return err
			} else if num <= 0 {
				return fmt.Errorf("value %d is out of allowable range", num)
			} else if num > uint32(c.Committee.Len()) {
				return fmt.Errorf("value exceeds %q size: %d > %d", committeeKey, num, c.Committee.Len())
			}
			c.ValidatorsHistory[uint32(height)] = num
			return nil
		})
		if err != nil {
			return c, err
		}
	}

	const rpcSection = cfgPathFSChainLocalConsensus + ".rpc"
	if v.IsSet(rpcSection) {
		c.RPC.Addresses, err = parseConfigAddressesTCP(v, rpcSection+".listen", "network addresses to listen insecure Neo RPC on", rpcDefaultListenPort)
		if err != nil && !errors.Is(err, errMissingConfig) {
			return c, err
		}

		const rpcTLSSection = rpcSection + ".tls"
		if v.GetBool(rpcTLSSection + ".enabled") {
			c.RPC.TLSConfig.Enabled = true

			c.RPC.TLSConfig.Addresses, err = parseConfigAddressesTCP(v, rpcTLSSection+".listen", "network addresses to listen to Neo RPC over TLS", rpcDefaultListenPort)
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

	minPeersConfigured := false
	const p2pSection = cfgPathFSChainLocalConsensus + ".p2p"
	if v.IsSet(p2pSection) {
		c.P2P.DialTimeout, err = parseConfigDurationPositive(v, p2pSection+".dial_timeout", "P2P dial timeout")
		if err != nil && !errors.Is(err, errMissingConfig) {
			return c, err
		}
		c.P2P.ProtoTickInterval, err = parseConfigDurationPositive(v, p2pSection+".proto_tick_interval", "P2P protocol tick interval")
		if err != nil && !errors.Is(err, errMissingConfig) {
			return c, err
		}
		c.P2P.ListenAddresses, err = parseConfigAddressesTCP(v, p2pSection+".listen", "network addresses to listen Neo P2P on", p2pDefaultListenPort)
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
				// defaults below
			} else {
				c.P2P.MinPeers = uint(minPeers)
				// zero can be set explicitly, so prevent overriding it
				minPeersConfigured = true
			}
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
	if !minPeersConfigured {
		n := uint(len(c.Committee))
		c.P2P.MinPeers = n - (n-1)/3 - 1
	}

	c.SetRolesInGenesis, err = parseConfigBool(v, cfgPathFSChainLocalConsensus+".set_roles_in_genesis", "flag to set roles for the committee in the genesis block")
	if err != nil && !errors.Is(err, errMissingConfig) {
		return c, err
	}

	c.Logger = _logger

	return c, nil
}

// sets NeoFS network settings to be used for FS chain
// auto-deployment.
func setNetworkSettingsDefaults(netCfg *deploy.NetworkConfiguration) {
	netCfg.MaxObjectSize = 64 << 20 // in bytes of object payload
	netCfg.EpochDuration = 240      // in FS chain blocks (e.g. ~1h for 15s block interval)
	netCfg.StoragePrice = 0         // in GAS per 1GB (NeoFS Balance contract's decimals)
	netCfg.AuditFee = 0             // in GAS per audit (NeoFS Balance contract's decimals)
	netCfg.ContainerFee = 1000      // in GAS per container (NeoFS Balance contract's decimals)
	netCfg.ContainerAliasFee = 500  // in GAS per container (NeoFS Balance contract's decimals)
	netCfg.IRCandidateFee = 0       // in GAS per candidate (Fixed8)
	netCfg.WithdrawalFee = 0        // in GAS per withdrawal (Fixed8)
	netCfg.EigenTrustIterations = 4
	netCfg.EigenTrustAlpha = 0.1
	netCfg.HomomorphicHashingDisabled = false
	netCfg.MaintenanceModeAllowed = false
}

type nnsConfig struct {
	systemEmail string
}

func parseNNSConfig(v *viper.Viper) (c nnsConfig, err error) {
	const rootSection = "nns"

	c.systemEmail, err = parseConfigString(v, rootSection+".system_email", "system email for NNS")
	if errors.Is(err, errMissingConfig) {
		err = nil
	}

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

func parseConfigUint64Range(v *viper.Viper, key, desc string, minV, maxV uint64) (uint64, error) {
	return parseConfigUint64Condition(v, key, desc, func(val uint64) error {
		if val < minV || val > maxV {
			return fmt.Errorf("out of allowable range [%d:%d]", minV, maxV)
		}
		return nil
	})
}

func parseConfigUint64Max(v *viper.Viper, key, desc string, maxV uint64) (uint64, error) {
	return parseConfigUint64Range(v, key, desc, 0, maxV)
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

func parseConfigAddressesTCP(v *viper.Viper, key, desc string, defaultPort string) ([]string, error) {
	ss, err := parseConfigStrings(v, key, desc)
	if err != nil {
		return nil, err
	}
	for i := range ss {
		if !strings.Contains(ss[i], ":") {
			ss[i] += ":" + defaultPort
		}
		_, err = net.ResolveTCPAddr("tcp", ss[i])
		if err != nil {
			return ss, fmt.Errorf("invalid %s '%s' (TCP addresses): %w", desc, key, err)
		}
	}
	return ss, nil
}

func parseConfigMap(v *viper.Viper, key, desc string, f func(name string, val any) error) error {
	var err error
	if !v.IsSet(key) {
		err = errMissingConfig
	}
	if err == nil {
		var m map[string]any
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
	return res, parseConfigMap(v, key, desc, func(name string, val any) error {
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
