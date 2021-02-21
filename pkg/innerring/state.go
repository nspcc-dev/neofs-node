package innerring

import (
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
	return s.innerRingIndex.Load() >= 0
}

// Index is a getter for a global index of node in inner ring list. Negative
// index means that node is not in the inner ring list.
func (s *Server) Index() int {
	return int(s.innerRingIndex.Load())
}

// InnerRingSize is a getter for a global size of inner ring list. This value
// paired with inner ring index.
func (s *Server) InnerRingSize() int {
	return int(s.innerRingSize.Load())
}

func (s *Server) voteForSidechainValidator(validators []keys.PublicKey) error {
	index := s.Index()
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
// from blockchain and then calls vote method on corresponding alphabet contract.
func (s *Server) InitAndVoteForSidechainValidator(validators []keys.PublicKey) error {
	err := s.initConfigFromBlockchain()
	if err != nil {
		return err
	}

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
