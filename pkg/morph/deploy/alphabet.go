package deploy

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

// initAlphabetPrm groups parameters of Alphabet members initialization.
type initAlphabetPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	committee keys.PublicKeys
	localAcc  *wallet.Account
}

// initAlphabet designates NeoFS Alphabet role to all committee members on the
// given Blockchain.
func initAlphabet(ctx context.Context, prm initAlphabetPrm) error {
	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	committeeActor, err := newCommitteeNotaryActor(prm.blockchain, prm.localAcc, prm.committee)
	if err != nil {
		return fmt.Errorf("create Notary service client sending transactions to be signed by the committee: %w", err)
	}

	roleContract := rolemgmt.New(committeeActor)
	txMonitor := newTransactionGroupMonitor(committeeActor)

	for ; ; prm.monitor.waitForNextBlock(ctx) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for NeoFS Alphabet role to be designated for the committee: %w", ctx.Err())
		default:
		}

		prm.logger.Info("checking NeoFS Alphabet role of the committee members...")

		accsWithAlphabetRole, err := roleContract.GetDesignatedByRole(noderoles.NeoFSAlphabet, prm.monitor.currentHeight())
		if err != nil {
			prm.logger.Error("failed to check role of the committee, will try again later", zap.Error(err))
			continue
		}

		someoneWithoutRole := len(accsWithAlphabetRole) < len(prm.committee)
		if !someoneWithoutRole {
			for i := range prm.committee {
				if !accsWithAlphabetRole.Contains(prm.committee[i]) {
					someoneWithoutRole = true
					break
				}
			}
		}
		if !someoneWithoutRole {
			prm.logger.Info("all committee members have a NeoFS Alphabet role")
			return nil
		}

		prm.logger.Info("not all members of the committee have a NeoFS Alphabet role, designation is needed")

		if txMonitor.isPending() {
			prm.logger.Info("previously sent Notary request designating NeoFS Alphabet role to the committee is still pending, will wait for the outcome")
			continue
		}

		mainTxID, fallbackTxID, vub, err := committeeActor.Notarize(
			roleContract.DesignateAsRoleTransaction(noderoles.NeoFSAlphabet, prm.committee))
		if err != nil {
			if errors.Is(err, neorpc.ErrInsufficientFunds) {
				prm.logger.Info("insufficient Notary balance to send new Notary request designating NeoFS Alphabet role to the committee, skip")
			} else {
				prm.logger.Error("failed to send new Notary request designating NeoFS Alphabet role to the committee, skip", zap.Error(err))
			}
			continue
		}

		prm.logger.Info("Notary request designating NeoFS Alphabet role to the committee has been successfully sent, will wait for the outcome",
			zap.Stringer("main tx", mainTxID), zap.Stringer("fallback tx", fallbackTxID), zap.Uint32("vub", vub))

		txMonitor.trackPendingTransactionsAsync(ctx, vub, mainTxID, fallbackTxID)
	}
}
