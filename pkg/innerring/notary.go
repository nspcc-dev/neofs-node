package innerring

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"go.uber.org/zap"
)

type (
	notaryConfig struct {
		disabled bool // true if notary disabled on chain
	}
)

const (
	// gasMultiplier defines how many times more the notary
	// balance must be compared to the GAS balance of the IR:
	//     notaryBalance = GASBalance * gasMultiplier.
	gasMultiplier = 3

	// gasDivisor defines what part of GAS balance (1/gasDivisor)
	// should be transferred to the notary service.
	gasDivisor = 2
)

func (s *Server) depositMainNotary() error {
	depositAmount, err := client.CalculateNotaryDepositAmount(s.mainnetClient, gasMultiplier, gasDivisor)
	if err != nil {
		return fmt.Errorf("could not calculate main notary deposit amount: %w", err)
	}

	return s.mainnetClient.DepositNotary(
		depositAmount,
		uint32(s.epochDuration.Load())+notaryExtraBlocks,
	)
}

func (s *Server) depositSideNotary() error {
	depositAmount, err := client.CalculateNotaryDepositAmount(s.morphClient, gasMultiplier, gasDivisor)
	if err != nil {
		return fmt.Errorf("could not calculate side notary deposit amount: %w", err)
	}

	return s.morphClient.DepositEndlessNotary(depositAmount)
}

func (s *Server) notaryHandler(_ event.Event) {
	if !s.mainNotaryConfig.disabled {
		err := s.depositMainNotary()
		if err != nil {
			s.log.Error("can't make notary deposit in main chain", zap.Error(err))
		}
	}

	err := s.depositSideNotary()
	if err != nil {
		s.log.Error("can't make notary deposit in side chain", zap.Error(err))
	}
}
