package metachain

import (
	"fmt"

	neogoconfig "github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/contracts"
	"go.uber.org/zap"
)

// TODO
func newCustomNatives(cfg neogoconfig.ProtocolConfiguration) []interop.Contract {
	var (
		defaultContracts = native.NewDefaultContracts(cfg)
		newContracts     = make([]interop.Contract, 0)

		neoContract native.INEO
	)
	for _, contract := range defaultContracts {
		switch contract.(type) {
		case *native.NEO:
			neoContract = contract.(native.INEO)
			newContracts = append(newContracts, neoContract)
		case *native.Management, *native.Ledger, *native.GAS, *native.Policy, *native.Designate, *native.Notary:
			newContracts = append(newContracts, contract)
		case *native.Std, *native.Crypto, *native.Oracle:
		default:
			panic(fmt.Sprintf("unexpected native contract found: %T", contract))
		}
	}
	return append(newContracts, contracts.MetaDataContract(neoContract))
}

// TODO
func NewMetaChain(cfg *config.Consensus, wallet *config.Wallet, errChan chan<- error, log *zap.Logger) (*blockchain.Blockchain, error) {
	return blockchain.New(cfg, wallet, errChan, log, newCustomNatives)
}
