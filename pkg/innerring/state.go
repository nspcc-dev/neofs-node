package innerring

import (
	"fmt"
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	auditwrp "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	control "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	"github.com/nspcc-dev/neofs-node/pkg/util/state"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const voteMethod = "vote"

var (
	persistateMainChainLastBlockKey = []byte("main_chain_last_processed_block")
	persistateSideChainLastBlockKey = []byte("side_chain_last_processed_block")
)

// EpochCounter is a getter for a global epoch counter.
func (s *Server) EpochCounter() uint64 {
	return s.epochCounter.Load()
}

// SetEpochCounter is a setter for contract processors to update global
// epoch counter.
func (s *Server) SetEpochCounter(val uint64) {
	s.epochCounter.Store(val)
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
		s.log.Error("can't get inner ring index", zap.String("error", err.Error()))
		return -1
	}

	return int(index)
}

// InnerRingSize is a getter for a global size of inner ring list. This value
// paired with inner ring index.
func (s *Server) InnerRingSize() int {
	size, err := s.statusIndex.InnerRingSize()
	if err != nil {
		s.log.Error("can't get inner ring size", zap.String("error", err.Error()))
		return 0
	}

	return int(size)
}

// AlphabetIndex is a getter for a global index of node in alphabet list.
// Negative index means that node is not in the alphabet list.
func (s *Server) AlphabetIndex() int {
	index, err := s.statusIndex.AlphabetIndex()
	if err != nil {
		s.log.Error("can't get alphabet index", zap.String("error", err.Error()))
		return -1
	}

	return int(index)
}

func (s *Server) voteForSidechainValidator(prm governance.VoteValidatorPrm) error {
	validators := prm.Validators

	index := s.InnerRingIndex()
	if s.contracts.alphabet.indexOutOfRange(index) {
		s.log.Info("ignore validator vote: node not in alphabet range")

		return nil
	}

	if len(validators) == 0 {
		s.log.Info("ignore validator vote: empty validators list")

		return nil
	}

	epoch := s.EpochCounter()

	var (
		nonce uint32 = 1
		vub   uint32
		vubP  *uint32
		err   error
	)

	if prm.Hash != nil {
		nonce, vub, err = s.morphClient.CalculateNonceAndVUB(*prm.Hash)
		if err != nil {
			return fmt.Errorf("could not calculate nonce and `validUntilBlock` values: %w", err)
		}
		vubP = &vub
	}

	s.contracts.alphabet.iterate(func(letter GlagoliticLetter, contract util.Uint160) {
		err := s.morphClient.NotaryInvoke(contract, s.feeConfig.SideChainFee(), nonce, vubP, voteMethod, int64(epoch), validators)
		if err != nil {
			s.log.Warn("can't invoke vote method in alphabet contract",
				zap.Int8("alphabet_index", int8(letter)),
				zap.Uint64("epoch", epoch),
				zap.String("error", err.Error()))
		}
	})

	return nil
}

// VoteForSidechainValidator calls vote method on alphabet contracts with
// provided list of keys.
func (s *Server) VoteForSidechainValidator(prm governance.VoteValidatorPrm) error {
	sort.Sort(prm.Validators)
	return s.voteForSidechainValidator(prm)
}

// WriteReport composes audit result structure from audit report
// and sends it to Audit contract.
func (s *Server) WriteReport(r *audit.Report) error {
	res := r.Result()
	res.SetPublicKey(s.pubKey)

	prm := auditwrp.PutPrm{}
	prm.SetResult(res)

	return s.auditClient.PutAuditResult(prm)
}

// ResetEpochTimer resets block timer that produces events to update epoch
// counter in netmap contract. Used to synchronize this even production
// based on block with notification of last epoch.
func (s *Server) ResetEpochTimer() error {
	return s.epochTimer.Reset()
}

func (s *Server) setHealthStatus(hs control.HealthStatus) {
	s.healthStatus.Store(hs)
}

// HealthStatus returns current health status of IR application.
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
