package innerring

import (
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"go.uber.org/zap"
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

func (s *Server) voteForSidechainValidator(validators keys.PublicKeys) error {
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

	s.contracts.alphabet.iterate(func(letter glagoliticLetter, contract util.Uint160) {
		err := invoke.AlphabetVote(s.morphClient, contract, epoch, validators)
		if err != nil {
			s.log.Warn("can't invoke vote method in alphabet contract",
				zap.Int8("alphabet_index", int8(letter)),
				zap.Uint64("epoch", epoch))
		}
	})

	return nil
}

// InitAndVoteForSidechainValidator is a public function to use outside of
// inner ring daemon execution. It initialize inner ring structure with data
// from blockchain and then calls vote method on alphabet contracts.
func (s *Server) InitAndVoteForSidechainValidator(validators keys.PublicKeys) error {
	err := s.initConfigFromBlockchain()
	if err != nil {
		return err
	}

	return s.VoteForSidechainValidator(validators)
}

// VoteForSidechainValidator calls vote method on alphabet contracts with
// provided list of keys.
func (s *Server) VoteForSidechainValidator(validators keys.PublicKeys) error {
	sort.Sort(validators)
	return s.voteForSidechainValidator(validators)
}

// WriteReport composes audit result structure from audit report
// and sends it to Audit contract.
func (s *Server) WriteReport(r *audit.Report) error {
	res := r.Result()
	res.SetPublicKey(s.pubKey)

	return s.auditClient.PutAuditResult(res)
}

// ResetEpochTimer resets block timer that produces events to update epoch
// counter in netmap contract. Used to synchronize this even production
// based on block with notification of last epoch.
func (s *Server) ResetEpochTimer() error {
	return s.epochTimer.Reset()
}
