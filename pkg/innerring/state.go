package innerring

import (
	"fmt"
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const voteMethod = "vote"

var (
	persistateMainChainLastBlockKey           = []byte("main_chain_last_processed_block")
	persistateFSChainLastBlockKey             = []byte("fs_chain_last_processed_block")
	persistateDeprecatedSidechainLastBlockKey = []byte("side_chain_last_processed_block")
)

// EpochCounter is a getter for a global epoch counter.
func (s *Server) EpochCounter() uint64 {
	return s.epochCounter.Load()
}

// SetEpochCounter is a setter for contract processors to update global
// epoch counter.
func (s *Server) SetEpochCounter(val uint64) {
	s.epochCounter.Store(val)
	if s.metrics != nil {
		s.metrics.SetEpoch(val)
	}
}

// EpochDuration is a getter for a global epoch duration.
func (s *Server) EpochDuration() uint64 {
	return s.epochDuration.Load()
}

// SetEpochDuration is a setter for the Netmap processor to update global
// epoch duration.
func (s *Server) SetEpochDuration(val uint64) {
	s.epochDuration.Store(val)
}

// IsActive is a getter for a global active flag state.
func (s *Server) IsActive() bool {
	return s.InnerRingIndex() >= 0
}

// IsAlphabet is a getter for a global alphabet flag state.
func (s *Server) IsAlphabet() bool {
	return s.AlphabetIndex() >= 0
}

// InnerRingIndex is a getter for a global index of node in inner ring list. Negative
// index means that node is not in the inner ring list.
func (s *Server) InnerRingIndex() int {
	index, err := s.statusIndex.InnerRingIndex()
	if err != nil {
		s.log.Error("can't get inner ring index", zap.Error(err))
		return -1
	}

	return int(index)
}

// InnerRingSize is a getter for a global size of inner ring list. This value
// paired with inner ring index.
func (s *Server) InnerRingSize() int {
	size, err := s.statusIndex.InnerRingSize()
	if err != nil {
		s.log.Error("can't get inner ring size", zap.Error(err))
		return 0
	}

	return int(size)
}

// AlphabetIndex is a getter for a global index of node in alphabet list.
// Negative index means that node is not in the alphabet list.
func (s *Server) AlphabetIndex() int {
	index, err := s.statusIndex.AlphabetIndex()
	if err != nil {
		s.log.Error("can't get alphabet index", zap.Error(err))
		return -1
	}

	return int(index)
}

func (s *Server) voteForFSChainValidator(validators keys.PublicKeys, trigger *util.Uint256) error {
	index := s.InnerRingIndex()
	if index >= len(s.contracts.alphabet) {
		s.log.Info("ignore validator vote: node not in alphabet range")

		return nil
	}

	if len(validators) == 0 {
		s.log.Info("ignore validator vote: empty validators list")

		return nil
	}

	voted, err := s.alreadyVoted(validators)
	if err != nil {
		return fmt.Errorf("could not check validators state: %w", err)
	}

	if voted {
		return nil
	}

	epoch := s.EpochCounter()

	var (
		nonce uint32
		vub   uint32
		vubP  *uint32
		hash  util.Uint256
	)

	if trigger != nil {
		hash = *trigger
	}

	nonce, vub, err = s.morphClient.CalculateNonceAndVUB(hash)
	if err != nil {
		return fmt.Errorf("could not calculate nonce and `validUntilBlock` values: %w", err)
	}
	vubP = &vub

	for ind, contract := range s.contracts.alphabet {
		_, err := s.morphClient.NotaryInvoke(contract, 0, nonce, vubP, voteMethod, epoch, validators)
		if err != nil {
			s.log.Warn("can't invoke vote method in alphabet contract",
				zap.Int("alphabet_index", ind),
				zap.Uint64("epoch", epoch),
				zap.Error(err))
		}
	}

	return nil
}

func (s *Server) alreadyVoted(validatorsToVote keys.PublicKeys) (bool, error) {
	currentValidators := make(map[keys.PublicKey]struct{}, len(s.contracts.alphabet))
	for ind, contract := range s.contracts.alphabet {
		validator, err := s.morphClient.AccountVote(contract)
		if err != nil {
			return false, fmt.Errorf("receiving %s's vote: %w", client.NNSAlphabetContractName(ind), err)
		}

		if validator == nil {
			continue
		}

		currentValidators[*validator] = struct{}{}
	}

	for _, v := range validatorsToVote {
		if _, voted := currentValidators[*v]; !voted {
			return false, nil
		}
	}

	return true, nil
}

// VoteForFSChainValidator calls vote method on alphabet contracts with
// the provided list of keys and hash of the triggering transaction.
func (s *Server) VoteForFSChainValidator(validators keys.PublicKeys, trigger *util.Uint256) error {
	sort.Sort(validators)
	return s.voteForFSChainValidator(validators, trigger)
}

// WriteReport composes the audit result structure from the audit report
// and sends it to Audit contract.
func (s *Server) WriteReport(r *audit.Report) error {
	res := r.Result()
	res.AuditorPublicKey = s.pubKey

	prm := auditClient.PutPrm{}
	prm.SetResult(res)

	return s.auditClient.PutAuditResult(prm)
}

// ResetEpochTimer resets the block timer that produces events to update epoch
// counter in the netmap contract. It is used to synchronize this even production
// based on the block with a notification of the last epoch.
func (s *Server) ResetEpochTimer(h uint32) error {
	if initEpochTimer := s.initEpochTimer.Swap(nil); initEpochTimer != nil {
		initEpochTimer.Stop()
	}
	s.epochTimer.Tick(h)
	return s.epochTimer.Reset()
}

func (s *Server) setHealthStatus(hs control.HealthStatus) {
	s.healthStatus.Store(hs)
	if s.metrics != nil {
		s.metrics.SetHealthCheck(int32(hs))
	}
}

// HealthStatus returns the current health status of the IR application.
func (s *Server) HealthStatus() control.HealthStatus {
	return s.healthStatus.Load().(control.HealthStatus)
}

func initPersistentStateStorage(cfg *viper.Viper) (*state.PersistentStorage, error) {
	persistPath := cfg.GetString("node.persistent_state.path")
	persistStorage, err := state.NewPersistentStorage(persistPath)
	if err != nil {
		return nil, fmt.Errorf("persistent state init error: %w", err)
	}

	return persistStorage, nil
}
