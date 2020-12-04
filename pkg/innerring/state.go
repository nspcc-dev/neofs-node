package innerring

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
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
func (s *Server) Index() int32 {
	return s.innerRingIndex.Load()
}

func (s *Server) voteForSidechainValidator(validators []keys.PublicKey) error {
	index := s.Index()
	if index < 0 || index >= alphabetContractsN {
		s.log.Info("ignore validator vote: node not in alphabet range")

		return nil
	}

	if len(validators) == 0 {
		s.log.Info("ignore validator vote: empty validators list")

		return nil
	}

	epoch := s.EpochCounter()

	for i := range s.contracts.alphabet {
		err := invoke.AlphabetVote(s.morphClient, s.contracts.alphabet[i], epoch, validators)
		if err != nil {
			s.log.Warn("can't invoke vote method in alphabet contract",
				zap.Int("alphabet_index", i),
				zap.Uint64("epoch", epoch))
		}
	}

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
