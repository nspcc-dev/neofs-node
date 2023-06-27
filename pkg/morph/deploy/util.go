package deploy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Error functions won't be needed when proper Neo status codes will arrive
// Track https://github.com/nspcc-dev/neofs-node/issues/2285

func isErrContractNotFound(err error) bool {
	return strings.Contains(err.Error(), "Unknown contract")
}

func isErrNotEnoughGAS(err error) bool {
	return errors.Is(err, neorpc.ErrValidationFailed) && strings.Contains(err.Error(), "insufficient funds")
}

func isErrInvalidTransaction(err error) bool {
	return errors.Is(err, neorpc.ErrValidationFailed)
}

func setGroupInManifest(_manifest *manifest.Manifest, _nef nef.File, groupPrivKey *keys.PrivateKey, deployerAcc util.Uint160) {
	contractAddress := state.CreateContractHash(deployerAcc, _nef.Checksum, _manifest.Name)
	sig := groupPrivKey.Sign(contractAddress.BytesBE())
	groupPubKey := groupPrivKey.PublicKey()

	ind := -1

	for i := range _manifest.Groups {
		if _manifest.Groups[i].PublicKey.Equal(groupPubKey) {
			ind = i
			break
		}
	}

	if ind >= 0 {
		_manifest.Groups[ind].Signature = sig
		return
	}

	_manifest.Groups = append(_manifest.Groups, manifest.Group{
		PublicKey: groupPubKey,
		Signature: sig,
	})
}

// blockchainMonitor is a thin utility around Blockchain providing state
// monitoring.
type blockchainMonitor struct {
	logger *zap.Logger

	blockchain Blockchain

	blockInterval time.Duration

	subID  string
	height atomic.Uint32
}

// newBlockchainMonitor constructs and runs monitor for the given Blockchain.
// Resulting blockchainMonitor must be stopped when no longer needed.
func newBlockchainMonitor(l *zap.Logger, b Blockchain) (*blockchainMonitor, error) {
	ver, err := b.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("request Neo protocol configuration: %w", err)
	}

	initialBlock, err := b.GetBlockCount()
	if err != nil {
		return nil, fmt.Errorf("get current blockchain height: %w", err)
	}

	blockCh := make(chan *block.Block)

	newBlockSubID, err := b.ReceiveBlocks(nil, blockCh)
	if err != nil {
		return nil, fmt.Errorf("subscribe to new blocks of the chain: %w", err)
	}

	res := &blockchainMonitor{
		logger:        l,
		blockchain:    b,
		blockInterval: time.Duration(ver.Protocol.MillisecondsPerBlock) * time.Millisecond,
		subID:         newBlockSubID,
	}

	res.height.Store(initialBlock)

	go func() {
		l.Info("listening to new blocks...")
		for {
			b, ok := <-blockCh
			if !ok {
				l.Info("listening to new blocks stopped")
				return
			}

			res.height.Store(b.Index)

			l.Info("new block arrived", zap.Uint32("height", b.Index))
		}
	}()

	return res, nil
}

// currentHeight returns current blockchain height.
func (x *blockchainMonitor) currentHeight() uint32 {
	return x.height.Load()
}

// waitForNextBlock blocks until blockchainMonitor encounters new block on the
// chain or provided context is done.
func (x *blockchainMonitor) waitForNextBlock(ctx context.Context) {
	initialBlock := x.currentHeight()

	ticker := time.NewTicker(x.blockInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if x.height.Load() > initialBlock {
				return
			}
		}
	}
}

// stop stops running blockchainMonitor. Stopped blockchainMonitor must not be
// used anymore.
func (x *blockchainMonitor) stop() {
	err := x.blockchain.Unsubscribe(x.subID)
	if err != nil {
		x.logger.Warn("failed to cancel subscription to new blocks", zap.Error(err))
	}
}

// readNNSOnChainState reads state of the NeoFS NNS contract in the given
// Blockchain. Returns both nil if contract is missing.
func readNNSOnChainState(b Blockchain) (*state.Contract, error) {
	// NNS must always have ID=1 in the NeoFS Sidechain
	const nnsContractID = 1
	res, err := b.GetContractStateByID(nnsContractID)
	if err != nil {
		if isErrContractNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read contract state by ID=%d: %w", nnsContractID, err)
	}
	return res, nil
}
