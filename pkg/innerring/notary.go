package innerring

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/spf13/viper"
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
	//     notaryBalance = GASBalance * gasMultiplier
	gasMultiplier = 3

	// gasDivisor defines what part of GAS balance (1/gasDivisor)
	// should be transferred to the notary service
	gasDivisor = 2
)

func (s *Server) depositMainNotary() (tx util.Uint256, err error) {
	depositAmount, err := client.CalculateNotaryDepositAmount(s.mainnetClient, gasMultiplier, gasDivisor)
	if err != nil {
		return util.Uint256{}, fmt.Errorf("could not calculate main notary deposit amount: %w", err)
	}

	return s.mainnetClient.DepositNotary(
		depositAmount,
		uint32(s.epochDuration.Load())+notaryExtraBlocks,
	)
}

func (s *Server) depositSideNotary() (tx util.Uint256, err error) {
	depositAmount, err := client.CalculateNotaryDepositAmount(s.morphClient, gasMultiplier, gasDivisor)
	if err != nil {
		return util.Uint256{}, fmt.Errorf("could not calculate side notary deposit amount: %w", err)
	}

	return s.morphClient.DepositNotary(
		depositAmount,
		uint32(s.epochDuration.Load())+notaryExtraBlocks,
	)
}

func (s *Server) notaryHandler(_ event.Event) {
	if !s.mainNotaryConfig.disabled {
		_, err := s.depositMainNotary()
		if err != nil {
			s.log.Error("can't make notary deposit in main chain", zap.Error(err))
		}
	}

	if !s.sideNotaryConfig.disabled {
		_, err := s.depositSideNotary()
		if err != nil {
			s.log.Error("can't make notary deposit in side chain", zap.Error(err))
		}
	}
}

func (s *Server) awaitMainNotaryDeposit(ctx context.Context, tx util.Uint256) error {
	return awaitNotaryDepositInClient(ctx, s.mainnetClient, tx)
}

func (s *Server) awaitSideNotaryDeposit(ctx context.Context, tx util.Uint256) error {
	return awaitNotaryDepositInClient(ctx, s.morphClient, tx)
}

func (s *Server) initNotary(ctx context.Context, deposit depositor, await awaiter, msg string) error {
	tx, err := deposit()
	if err != nil {
		return err
	}

	s.log.Info(msg)

	return await(ctx, tx)
}

func awaitNotaryDepositInClient(ctx context.Context, cli *client.Client, txHash util.Uint256) error {
	for i := 0; i < notaryDepositTimeout; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		ok, err := cli.TxHalt(txHash)
		if err == nil {
			if ok {
				return nil
			}

			return errDepositFail
		}

		_ = cli.Wait(ctx, 1)
	}

	return errDepositTimeout
}

func parseNotaryConfigs(cfg *viper.Viper, withSideNotary, withMainNotary bool) (main, side *notaryConfig) {
	main = new(notaryConfig)
	side = new(notaryConfig)

	if !withSideNotary {
		main.disabled = true
		side.disabled = true

		return
	}

	main.disabled = !withMainNotary

	return
}
