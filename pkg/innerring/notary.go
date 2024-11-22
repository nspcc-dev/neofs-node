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

	till := uint32(s.epochDuration.Load()) + notaryExtraBlocks
	s.log.Debug("making main chain notary deposit",
		zap.Stringer("fixed8 deposit", depositAmount),
		zap.Uint32("till", till))

	return s.mainnetClient.DepositNotary(depositAmount, till)
}

func (s *Server) depositFSNotary() error {
	depositAmount, err := client.CalculateNotaryDepositAmount(s.morphClient, gasMultiplier, gasDivisor)
	if err != nil {
		return fmt.Errorf("could not calculate FS notary deposit amount: %w", err)
	}

	s.log.Debug("making FS chain endless notary deposit", zap.Stringer("fixed8 deposit", depositAmount))

	return s.morphClient.DepositEndlessNotary(depositAmount)
}

func (s *Server) notaryHandler(_ event.Event) {
	if !s.mainNotaryConfig.disabled {
		err := s.depositMainNotary()
		if err != nil {
			s.log.Error("can't make notary deposit in main chain", zap.Error(err))
		}
	}

	err := s.depositFSNotary()
	if err != nil {
		s.log.Error("can't make notary deposit in FS chain", zap.Error(err))
	}
}
