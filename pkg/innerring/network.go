package innerring

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"go.uber.org/zap"
)

func (s *Server) ListNotaryRequests() ([]string, error) {
	//txs := s.bc.GetVerifiedTransactions()
	a, err := s.netmapClient.Morph().GetRawMemPool()
	if err != nil {
		return nil, err
	}
	s.log.Debug("mem pool hashes", zap.String("hashes", fmt.Sprintf("%x", a)))

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

func (s *Server) RequestNotary(script []byte) (string, error) {
	//epoch := s.EpochCounter()
	//
	//var (
	//	nonce uint32 = 1
	//	vubP  *uint32
	//)
	//
	//err := s.netmapClient.Morph().NotaryInvoke(s.netmapClient.ContractAddress(), 0, nonce, vubP, "newEpoch", epoch+1)
	//if err != nil {
	//	s.log.Warn("can't invoke newEpoch method in alphabet contract",
	//		zap.Uint64("epoch", epoch),
	//		zap.Error(err))
	//}
	// --------------------------------------------------------
	//tx := transaction.New(script, 0)
	//err := s.netmapClient.Morph().NotarySignAndInvokeTX(tx)
	//if err != nil {
	//	return "", fmt.Errorf("failed to invoke notary tx: %w", err)
	//}
	// ---------------------------------------------------------
	validators := s.predefinedValidators
	index := s.InnerRingIndex()
	if s.contracts.alphabet.indexOutOfRange(index) {
		s.log.Info("ignore validator vote: node not in alphabet range")

		return "", nil
	}

	if len(validators) == 0 {
		s.log.Info("ignore validator vote: empty validators list")

		return "", nil
	}

	voted, err := s.alreadyVoted(validators)
	if err != nil {
		return "", fmt.Errorf("could not check validators state: %w", err)
	}

	if voted {
		return "", nil
	}

	epoch := s.EpochCounter()

	var (
		nonce uint32 = 1
		vubP  *uint32
	)

	s.contracts.alphabet.iterate(func(ind int, contract util.Uint160) {
		err := s.morphClient.NotaryInvoke(contract, 0, nonce, vubP, voteMethod, epoch, validators)
		if err != nil {
			s.log.Warn("can't invoke vote method in alphabet contract",
				zap.Int("alphabet_index", ind),
				zap.Uint64("epoch", epoch),
				zap.Error(err))
		}
	})

	return "", nil
}

func (s *Server) SignNotary(hash util.Uint256) (string, error) {
	tx, err := s.netmapClient.Morph().GetRawNotaryTransactionVerbose(hash)
	if err != nil {
		return "", err
	}

	js, err := json.Marshal(tx)
	if err != nil {
		return "", err
	}
	s.log.Debug(fmt.Sprintf("tx json %s", string(js)))

	for i, w := range tx.Scripts {
		s.log.Debug(fmt.Sprintf("signed notary transaction %d, script %s\n len %d, has prefix: %v, is first right %v, is second norm %v",
			i, w.InvocationScript, len(w.InvocationScript), bytes.HasPrefix(w.InvocationScript, []byte{byte(opcode.PUSHDATA1), keys.SignatureLen}),
			bytes.HasPrefix(w.InvocationScript, []byte{byte(opcode.PUSHDATA1)}), bytes.Index(w.InvocationScript, []byte{keys.SignatureLen}) == 1))
	}

	err = s.netmapClient.Morph().NotarySignAndInvokeTX(tx)
	if err != nil {
		return "", err
	}

	//ps := make([]smartcontract.Parameter, 0, len(tx.Attributes))
	//for _, a := range tx.Attributes {
	//	m, err := a.MarshalJSON()
	//	if err != nil {
	//		return "", err
	//	}
	//
	//	ma := make(map[string]any)
	//	json.Unmarshal(m, &ma)
	//
	//	param := smartcontract.NewParameter(smartcontract.MapType)
	//	val := make([]smartcontract.ParameterPair, 0, len(ma)-1)
	//	for k, v := range ma {
	//		if k == "type" {
	//			continue
	//		}
	//		ke, err := smartcontract.NewParameterFromValue(k)
	//		if err != nil {
	//			return "", err
	//		}
	//		va, err := smartcontract.NewParameterFromValue(v)
	//		if err != nil {
	//			return "", err
	//		}
	//		val = append(val, smartcontract.ParameterPair{
	//			Key:   ke,
	//			Value: va,
	//		})
	//	}
	//	param.Value = val
	//
	//	s.log.Debug(fmt.Sprintf("attributes map: %v, json: %v", ma, string(m)))
	//	ps = append(ps, param)
	//}

	//s.log.Debug(fmt.Sprintf("parameters %v", ps))

	//res, err := s.netmapClient.Morph().InvokeContractVerify(s.netmapClient.ContractAddress(), ps, tx.Signers, tx.Scripts...)
	//if err != nil {
	//	return "", err
	//}

	//s.log.Debug(fmt.Sprintf("invoke res %x", res))

	return string(tx.Script), nil
}
