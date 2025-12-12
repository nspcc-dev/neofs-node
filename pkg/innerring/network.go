package innerring

import (
	"context"
	"crypto/elliptic"
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"go.uber.org/zap"
)

// ListNotaryRequests list hashes of all P2PNotaryRequest transactions
// that are currently in the RPC node's notary request pool.
func (s *Server) ListNotaryRequests() ([]util.Uint256, error) {
	hashesMap, err := s.netmapClient.Morph().GetRawNotaryPool()
	if err != nil {
		return nil, err
	}

	result := make([]util.Uint256, 0, len(hashesMap.Hashes))
	for k := range hashesMap.Hashes {
		result = append(result, k)
	}
	return result, nil
}

// RequestNotary create and send a notary request with
// the sign of current node with the following methods and args:
//
// - "newEpoch", transaction for creating of new NeoFS epoch event in FS chain, no args;
//
// - "setConfig", transaction to add/update global config value
// in the NeoFS network, 2 args - key and value UTF-8 encoded strings;
//
// - "removeNode", transaction to move nodes to the netmap.NodeStateOffline state
// in the candidates list, 1 arg is the public key of the node in binary form.
func (s *Server) RequestNotary(method string, args ...[]byte) (util.Uint256, error) {
	if !s.IsAlphabet() {
		s.log.Info("non alphabet mode, ignore request")
		return util.Uint256{}, errors.New("non alphabet mode, ignore request")
	}

	var (
		hash util.Uint256
		err  error
	)

	switch method {
	case "newEpoch":
		epoch := s.EpochCounter()

		hash, err = s.netmapClient.Morph().NotaryInvoke(context.TODO(), s.netmapClient.ContractAddress(), false, 0, 1, nil, method, epoch+1)
		if err != nil {
			s.log.Warn("external request: can't invoke newEpoch method in netmap",
				zap.Uint64("epoch", epoch),
				zap.Error(err))
		}
	case "setConfig":
		if len(args) < 2 {
			return util.Uint256{}, errors.New("invalid config pairs")
		}
		if len(args) > 2 {
			s.log.Warn("2 args expected: key and value; all others will be ignored",
				zap.String("method", method),
				zap.ByteStrings("args", args))
		}

		var v any
		k := string(args[0])
		v, err = convertKnownConfigValues(k, string(args[1]))
		if err != nil {
			return util.Uint256{}, err
		}

		hash, err = s.netmapClient.Morph().NotaryInvoke(context.TODO(), s.netmapClient.ContractAddress(), false, 0, 1, nil, method, nil, k, v)
		if err != nil {
			s.log.Warn("external request: can't invoke setConfig method in netmap",
				zap.String("key", k),
				zap.Any("value", v),
				zap.Error(err))
		}
	case "removeNode":
		if len(args) == 0 {
			return util.Uint256{}, errors.New("empty node key")
		}
		if len(args) > 1 {
			s.log.Warn("1 public key expected, all but the first one will be ignored",
				zap.String("method", method),
				zap.ByteStrings("args", args))
		}

		nodePubKey, err := keys.NewPublicKeyFromBytes(args[0], elliptic.P256())
		if err != nil {
			return util.Uint256{}, fmt.Errorf("can't parse node public key: %w", err)
		}

		hash, err = s.netmapClient.Morph().NotaryInvoke(context.TODO(),
			s.netmapClient.ContractAddress(), false, 0, 1, nil,
			"deleteNode", nodePubKey.Bytes())
		if err != nil {
			s.log.Warn("external request: can't invoke deleteNode method in netmap",
				zap.String("node pub key", nodePubKey.String()),
				zap.Error(err))
		}
	default:
		return util.Uint256{}, errors.New("invalid method")
	}

	return hash, err
}

// SignNotary get a notary transaction by its hash, then send it with sign of the current node.
func (s *Server) SignNotary(hash util.Uint256) error {
	tx, err := s.netmapClient.Morph().GetRawNotaryTransactionVerbose(hash)
	if err != nil {
		return err
	}

	ok, err := s.netmapClient.Morph().IsValidScript(tx.Script, tx.Signers)
	if err != nil {
		s.log.Warn("error in validation script",
			zap.String("hash", tx.Hash().StringLE()),
			zap.Error(err))
		return err
	}
	if !ok {
		s.log.Warn("non-halt notary transaction",
			zap.String("hash", tx.Hash().StringLE()),
			zap.Error(err))
		return err
	}

	// tx arrives already signed by another node,
	// and method `NotarySignAndInvokeTX` (more precisely, method actor.Sign inside)
	// works when there is no signature, so we delete it
	// TODO: delete after resolve https://github.com/nspcc-dev/neo-go/issues/3770
	for i, wit := range tx.Scripts {
		if len(wit.InvocationScript) != 0 {
			tx.Scripts[i].InvocationScript = tx.Scripts[i].InvocationScript[:0]
		}
	}

	err = s.netmapClient.Morph().NotarySignAndInvokeTX(tx, false)
	if err != nil {
		return err
	}

	return nil
}

func convertKnownConfigValues(k, v string) (any, error) {
	var (
		val any
		err error
	)
	switch k {
	case netmapcore.BasicIncomeRateConfig,
		netmapcore.ContainerFeeConfig, netmapcore.ContainerAliasFeeConfig,
		netmapcore.EigenTrustIterationsConfig,
		netmapcore.EpochDurationConfig,
		netmapcore.MaxObjectSizeConfig, netmapcore.WithdrawFeeConfig:
		val, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected int, got '%s'", k, v)
		}
	case netmapcore.EigenTrustAlphaConfig:
		// just check that it could
		// be parsed correctly
		_, err = strconv.ParseFloat(v, 64)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected float, got '%s'", k, v)
		}

		val = v
	case netmapcore.HomomorphicHashingDisabledKey:
		val, err = strconv.ParseBool(v)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected bool, got '%s'", k, v)
		}

	default:
		val = v
	}

	return val, err
}
