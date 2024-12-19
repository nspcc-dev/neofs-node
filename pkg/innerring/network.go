package innerring

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"go.uber.org/zap"
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

func (s *Server) RequestNotary(method string, args ...string) (string, error) {
	if !s.IsAlphabet() {
		s.log.Info("non alphabet mode, ignore request")
		return "", nil
	}

	var (
		nonce uint32 = 1
		vubP  *uint32
	)

	switch method {
	case "newEpoch":
		epoch := s.EpochCounter()

		err := s.netmapClient.Morph().NotaryInvoke(s.netmapClient.ContractAddress(), 0, nonce, vubP, method, epoch+1)
		if err != nil {
			s.log.Warn("can't invoke newEpoch method in alphabet contract",
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
			k, v := args[i], args[i+1]
			err := s.netmapClient.Morph().NotaryInvoke(s.netmapClient.ContractAddress(), 0, nonce, vubP, method, nil, k, v)
			if err != nil {
				s.log.Warn("can't invoke setConfig method in alphabet contract",
					zap.String("key", k),
					zap.String("value", v),
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
			err := s.netmapClient.Morph().NotaryInvoke(
				s.netmapClient.ContractAddress(), 0, nonce, vubP,
				"updateStateIR", netmap.NodeStateOffline, nodePubKey.Bytes())
			if err != nil {
				s.log.Warn("can't invoke updateSateIR method in alphabet contract",
					zap.String("node pub key", nodePubKey.String()),
					zap.Error(err))
			}
		}
	}

	return "", nil
}

func (s *Server) SignNotary(hash util.Uint256) (string, error) {
	tx, err := s.netmapClient.Morph().GetRawNotaryTransactionVerbose(hash)
	if err != nil {
		return "", err
	}

	ok, err := s.netmapClient.Morph().IsValidScript(tx.Script, tx.Signers)
	if err != nil || !ok {
		s.log.Warn("non-halt notary transaction",
			zap.String("hash", tx.Hash().StringLE()),
			zap.Error(err))
		return "", err
	}

	for i, wit := range tx.Scripts {
		if len(wit.InvocationScript) != 0 {
			tx.Scripts[i].InvocationScript = []byte("")
		}
	}

	err = s.netmapClient.Morph().NotarySignAndInvokeTX(tx)
	if err != nil {
		return "", err
	}

	return string(tx.Script), nil
}
