package innerring

import (
	"context"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/spf13/viper"
)

type (
	notaryConfig struct {
		amount   fixedn.Fixed8 // amount of deposited GAS to notary contract
		duration uint32        // lifetime of notary deposit in blocks
		disabled bool          // true if notary disabled on chain
	}
)

func (s *Server) depositMainNotary() (tx util.Uint256, err error) {
	return s.mainnetClient.DepositNotary(
		s.mainNotaryConfig.amount,
		s.mainNotaryConfig.duration+notaryExtraBlocks,
	)
}

func (s *Server) depositSideNotary() (tx util.Uint256, err error) {
	return s.morphClient.DepositNotary(
		s.sideNotaryConfig.amount,
		s.sideNotaryConfig.duration+notaryExtraBlocks,
	)
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

		cli.Wait(ctx, 1)
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
	main.amount = fixedn.Fixed8(cfg.GetInt64("notary.main.deposit_amount"))
	main.duration = cfg.GetUint32("timers.main_notary")

	side.amount = fixedn.Fixed8(cfg.GetInt64("notary.side.deposit_amount"))
	side.duration = cfg.GetUint32("timers.side_notary")

	return
}
