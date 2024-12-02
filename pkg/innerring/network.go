package innerring

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"go.uber.org/zap"
)

const (
	netmapEpochKey                   = "EpochDuration"
	netmapMaxObjectSizeKey           = "MaxObjectSize"
	netmapAuditFeeKey                = "AuditFee"
	netmapContainerFeeKey            = "ContainerFee"
	netmapContainerAliasFeeKey       = "ContainerAliasFee"
	netmapEigenTrustIterationsKey    = "EigenTrustIterations"
	netmapEigenTrustAlphaKey         = "EigenTrustAlpha"
	netmapBasicIncomeRateKey         = "BasicIncomeRate"
	netmapInnerRingCandidateFeeKey   = "InnerRingCandidateFee"
	netmapWithdrawFeeKey             = "WithdrawFee"
	netmapHomomorphicHashDisabledKey = "HomomorphicHashingDisabled"
	netmapMaintenanceAllowedKey      = "MaintenanceModeAllowed"
)

func (s *Server) ListNotaryRequests() ([]string, error) {
	hashesMap, err := s.netmapClient.Morph().GetRawNotaryPool()
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(hashesMap.Hashes))
	for k := range hashesMap.Hashes {
		result = append(result, k.String())
	}
	return result, nil
}

func (s *Server) RequestNotary(method string, args ...string) (hash string, err error) {
	if !s.IsAlphabet() {
		s.log.Info("non alphabet mode, ignore request")
		return "", errors.New("non alphabet mode, ignore request")
	}

	switch method {
	case "newEpoch":
		epoch := s.EpochCounter()

		hash, err = s.netmapClient.Morph().NotaryInvoke(s.netmapClient.ContractAddress(), 0, 1, nil, method, epoch+1)
		if err != nil {
			s.log.Warn("external request: can't invoke newEpoch method in netmap",
				zap.Uint64("epoch", epoch),
				zap.Error(err))
		}
	case "setConfig":
		if len(args) == 0 {
			return "", errors.New("empty config pairs")
		}
		if len(args)%2 != 0 {
			return "", errors.New("invalid config pairs")
		}

		for i := 0; i < len(args); i += 2 {
			var v any
			v, err = convertKnownConfigValues(args[i], args[i+1])
			if err != nil {
				return "", err
			}

			hash, err = s.netmapClient.Morph().NotaryInvoke(s.netmapClient.ContractAddress(), 0, 1, nil, method, nil, args[i], v)
			if err != nil {
				s.log.Warn("external request: can't invoke setConfig method in netmap",
					zap.String("key", args[i]),
					zap.Any("value", v),
					zap.Error(err))
			}
		}
	case "removeNode":
		if len(args) == 0 {
			return "", errors.New("empty node key")
		}

		nodeKeys := make(keys.PublicKeys, len(args))
		for i := range args {
			var err error
			nodeKeys[i], err = keys.NewPublicKeyFromString(args[i])
			if err != nil {
				return "", fmt.Errorf("can't parse node public key: %w", err)
			}
		}

		for _, nodePubKey := range nodeKeys {
			hash, err = s.netmapClient.Morph().NotaryInvoke(
				s.netmapClient.ContractAddress(), 0, 1, nil,
				"updateStateIR", netmap.NodeStateOffline, nodePubKey.Bytes())
			if err != nil {
				s.log.Warn("external request: can't invoke updateSateIR method in netmap",
					zap.String("node pub key", nodePubKey.String()),
					zap.Error(err))
			}
		}
	default:
		return "", errors.New("invalid method")
	}

	return
}

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

	err = s.netmapClient.Morph().NotarySignAndInvokeTX(tx)
	if err != nil {
		return err
	}

	return nil
}

func convertKnownConfigValues(k, v string) (val any, err error) {
	switch k {
	case netmapAuditFeeKey, netmapBasicIncomeRateKey,
		netmapContainerFeeKey, netmapContainerAliasFeeKey,
		netmapEigenTrustIterationsKey,
		netmapEpochKey, netmapInnerRingCandidateFeeKey,
		netmapMaxObjectSizeKey, netmapWithdrawFeeKey:
		val, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected int, got '%s'", k, v)
		}
	case netmapEigenTrustAlphaKey:
		// just check that it could
		// be parsed correctly
		_, err = strconv.ParseFloat(v, 64)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected float, got '%s'", k, v)
		}

		val = v
	case netmapHomomorphicHashDisabledKey, netmapMaintenanceAllowedKey:
		val, err = strconv.ParseBool(v)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected bool, got '%s'", k, v)
		}

	default:
		val = v
	}

	return
}
