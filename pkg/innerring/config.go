package innerring

import (
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/storage/dbconfig"
	"github.com/nspcc-dev/neofs-contract/deploy"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
)

// Various configuration paths.
var (
	cfgFSChainName               = "fschain"
	cfgPathFSChainLocalConsensus = cfgFSChainName + ".consensus"
)

// Default ports for listening on TCP addresses.
const (
	p2pDefaultListenPort = "20333"
	rpcDefaultListenPort = "30333"
)

// checks if Inner Ring app is configured to be launched in local consensus
// mode. Returns error if neither NeoFS chain RPC endpoints nor local consensus
// is configured.
func isLocalConsensusMode(cfg *config.Config) (bool, error) {
	endpointsUnset := cfg.FSChain.Endpoints == nil
	if endpointsUnset && !cfg.IsSet(cfgFSChainName+".consensus") {
		return false, fmt.Errorf("either '%s' or '%s' must be configured",
			cfgFSChainName+".endpoints", cfgFSChainName+".consensus")
	}

	return endpointsUnset, nil
}

func validateBlockchainConfig(cfg *config.Config) error {
	var err error
	cfgFSChainLocalConsensus := &cfg.FSChain.Consensus

	if !cfg.IsSet(cfgPathFSChainLocalConsensus) {
		return fmt.Errorf("missing root section '%s'", cfgPathFSChainLocalConsensus)
	}

	if cfgFSChainLocalConsensus.Magic == 0 {
		return fmt.Errorf("magic not set or zero '%s'", cfgPathFSChainLocalConsensus+".magic")
	}

	var storageSection = cfgPathFSChainLocalConsensus + ".storage"
	switch typ := cfgFSChainLocalConsensus.Storage.Type; typ {
	case dbconfig.InMemoryDB:
	case dbconfig.BoltDB, dbconfig.LevelDB:
		if cfgFSChainLocalConsensus.Storage.Path == "" {
			return fmt.Errorf("missing path to the BoltDB '%s'", storageSection+".path")
		}
	default:
		return fmt.Errorf("unsupported storage type '%s': '%s'", storageSection+".type", typ)
	}

	var committeeKey = cfgPathFSChainLocalConsensus + ".committee"
	if cfgFSChainLocalConsensus.Committee.Len() == 0 {
		return fmt.Errorf("empty committee members '%s'", committeeKey)
	}

	err = checkDurationPositive(cfgFSChainLocalConsensus.TimePerBlock, cfgPathFSChainLocalConsensus+".time_per_block")
	if err != nil {
		return err
	}

	err = checkDurationPositive(cfgFSChainLocalConsensus.MaxTimePerBlock, cfgPathFSChainLocalConsensus+".max_time_per_block")
	if err != nil {
		return err
	}

	if cfgFSChainLocalConsensus.MaxTimePerBlock != 0 && cfgFSChainLocalConsensus.MaxTimePerBlock < cfgFSChainLocalConsensus.TimePerBlock {
		return errors.New("consensus max_time_per_block set to lower value than time_per_block")
	}

	cfgFSChainLocalConsensus.SeedNodes, err = parseConfigAddressesTCP(cfgFSChainLocalConsensus.SeedNodes,
		cfgPathFSChainLocalConsensus+".seed_nodes", p2pDefaultListenPort, false)
	if err != nil {
		return err
	}

	for k := range cfgFSChainLocalConsensus.Hardforks.Name {
		if k == "" {
			return fmt.Errorf("missing hardforks name '%s'", cfgPathFSChainLocalConsensus+".hardforks")
		}
	}

	if len(cfgFSChainLocalConsensus.ValidatorsHistory.Height) > 0 {
		committeeSize := uint32(cfgFSChainLocalConsensus.Committee.Len())

		for k, v := range cfgFSChainLocalConsensus.ValidatorsHistory.Height {
			if k%committeeSize != 0 {
				return fmt.Errorf("height %d is not a multiple of the %q size", k, committeeKey)
			}
			if v <= 0 {
				return fmt.Errorf("value %d for %d is not a positive number", v, k)
			} else if v > committeeSize {
				return fmt.Errorf("value exceeds %q size: %d > %d", committeeKey, v, committeeSize)
			}
		}
	}

	var rpcSection = cfgPathFSChainLocalConsensus + ".rpc"
	if cfg.IsSet(rpcSection) {
		cfgFSChainLocalConsensus.RPC.Listen, err = parseConfigAddressesTCP(cfgFSChainLocalConsensus.RPC.Listen,
			rpcSection+".listen", rpcDefaultListenPort, false)
		if err != nil {
			return err
		}

		err = checkIntMax(cfgFSChainLocalConsensus.RPC.MaxWebSocketClients, rpcSection+".max_websocket_clients")
		if err != nil {
			return err
		}
		err = checkIntMax(cfgFSChainLocalConsensus.RPC.MaxGasInvoke, rpcSection+".max_gas_invoke")
		if err != nil {
			return err
		}
		err = checkIntMax(cfgFSChainLocalConsensus.RPC.SessionPoolSize, rpcSection+".session_pool_size")
		if err != nil {
			return err
		}

		var rpcTLSSection = rpcSection + ".tls"
		if cfgFSChainLocalConsensus.RPC.TLS.Enabled {
			rpcTLSListen := rpcTLSSection + ".listen"
			if len(cfgFSChainLocalConsensus.RPC.TLS.Listen) == 0 {
				return fmt.Errorf("missing tls listen section '%s'", rpcTLSListen)
			}
			cfgFSChainLocalConsensus.RPC.TLS.Listen, err = parseConfigAddressesTCP(cfgFSChainLocalConsensus.RPC.TLS.Listen,
				rpcTLSListen, rpcDefaultListenPort, false)
			if err != nil {
				return err
			}

			if strings.TrimSpace(cfgFSChainLocalConsensus.RPC.TLS.CertFile) == "" {
				return fmt.Errorf("RPC TLS setup is enabled but no certificate ('%s') is provided", rpcTLSSection+".cert_file")
			}

			if strings.TrimSpace(cfgFSChainLocalConsensus.RPC.TLS.KeyFile) == "" {
				return fmt.Errorf("RPC TLS setup is enabled but no key ('%s') is provided", rpcTLSSection+".key_file")
			}
		}
	}

	var p2pSection = cfgPathFSChainLocalConsensus + ".p2p"
	if cfg.IsSet(p2pSection) {
		err = checkDurationPositive(cfgFSChainLocalConsensus.P2P.DialTimeout, p2pSection+".dial_timeout")
		if err != nil {
			return err
		}
		err = checkDurationPositive(cfgFSChainLocalConsensus.P2P.ProtoTickInterval, p2pSection+".proto_tick_interval")
		if err != nil {
			return err
		}
		cfgFSChainLocalConsensus.P2P.Listen, err = parseConfigAddressesTCP(cfgFSChainLocalConsensus.P2P.Listen,
			p2pSection+".listen", p2pDefaultListenPort, true)
		if err != nil {
			return err
		}

		var p2pPeersSection = p2pSection + ".peers"
		err = checkIntMax(cfgFSChainLocalConsensus.P2P.Peers.Min, p2pPeersSection+".min")
		if err != nil {
			return err
		}
		err = checkIntMax(cfgFSChainLocalConsensus.P2P.Peers.Max, p2pPeersSection+".max")
		if err != nil {
			return err
		}
		err = checkIntMax(cfgFSChainLocalConsensus.P2P.Peers.Attempts, p2pPeersSection+".attempts")
		if err != nil {
			return err
		}

		var pingSection = p2pSection + ".ping"
		if cfg.IsSet(pingSection) {
			err = checkDurationPositive(cfgFSChainLocalConsensus.P2P.Ping.Interval, pingSection+".interval")
			if err != nil {
				return err
			}
			err = checkDurationPositive(cfgFSChainLocalConsensus.P2P.Ping.Timeout, pingSection+".timeout")
			if err != nil {
				return err
			}
		}
	}
	if !cfg.IsSet(p2pSection + ".peers.min") {
		n := uint32(cfgFSChainLocalConsensus.Committee.Len())
		cfgFSChainLocalConsensus.P2P.Peers.Min = n - (n-1)/3 - 1
	}

	return nil
}

// sets NeoFS network settings to be used for FS chain
// auto-deployment.
func setNetworkSettingsDefaults(netCfg *deploy.NetworkConfiguration) {
	netCfg.MaxObjectSize = 64 << 20 // in bytes of object payload
	netCfg.EpochDuration = 240      // in seconds
	netCfg.StoragePrice = 0         // in GAS per 1GB (NeoFS Balance contract's decimals)
	netCfg.ContainerFee = 1000      // in GAS per container (NeoFS Balance contract's decimals)
	netCfg.ContainerAliasFee = 500  // in GAS per container (NeoFS Balance contract's decimals)
	netCfg.WithdrawalFee = 0        // in GAS per withdrawal (Fixed8)
	netCfg.EigenTrustIterations = 4
	netCfg.EigenTrustAlpha = 0.1
	netCfg.HomomorphicHashingDisabled = false
	netCfg.MaintenanceModeAllowed = false
}

func checkDurationPositive(val time.Duration, key string) error {
	if val < 0 {
		return fmt.Errorf("invalid '%s' (duration): must be positive", key)
	}
	return nil
}

func checkIntMax(val uint32, key string) error {
	if val > math.MaxInt32 {
		return fmt.Errorf("invalid '%s' (int): greater than %d", key, math.MaxInt32)
	}

	return nil
}

func parseConfigAddressesTCP(ss []string, key string, defaultPort string, allowAnnounces bool) ([]string, error) {
	for i := range ss {
		_, _, err := net.SplitHostPort(ss[i])
		if err == nil {
			continue
		}
		// No easy way to check for "missing port error".
		var addr = ss[i] + ":" + defaultPort
		_, _, err = net.SplitHostPort(addr)
		if err == nil {
			ss[i] = addr
			continue
		}
		if allowAnnounces {
			var lastColon = strings.LastIndexByte(ss[i], ':')
			if lastColon != -1 {
				addr = ss[i][:lastColon]
				_, _, err = net.SplitHostPort(addr)
			}
		}
		if err != nil {
			return ss, fmt.Errorf("invalid '%s' (TCP addresses): %w", key, err)
		}
	}
	return ss, nil
}
