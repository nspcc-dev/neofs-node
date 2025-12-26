package metachain

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/contracts"
	"go.uber.org/zap"
)

// NewMetaChain returns side chain with redefined/custom native contracts.
// See [contracts.NewCustomNatives] for details.
func NewMetaChain(cfg *config.Consensus, wallet *config.Wallet, errChan chan<- error, log *zap.Logger) (*blockchain.Blockchain, error) {
	return blockchain.New(cfg, wallet, errChan, log, contracts.NewCustomNatives)
}
