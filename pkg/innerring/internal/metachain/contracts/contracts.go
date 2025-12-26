package contracts

import (
	"fmt"

	neogoconfig "github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/contracts/gas"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/contracts/meta"
)

// NewCustomNatives returns custom list of native contracts for metadata
// side chain. Returned contracts:
//   - NEO
//   - Management
//   - Ledger
//   - Policy
//   - Designate
//   - Notary
//   - redefined GAS (see [gas.NewGAS] for details)
//   - new native metadata contract (see [meta.NewMetadata] for details).
func NewCustomNatives(cfg neogoconfig.ProtocolConfiguration) []interop.Contract {
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
		case *native.GAS:
			newContracts = append(newContracts, gas.NewGAS(int64(cfg.InitialGASSupply)))
		case *native.Management, *native.Ledger, *native.Policy, *native.Designate, *native.Notary:
			newContracts = append(newContracts, contract)
		case *native.Std, *native.Crypto, *native.Oracle, *native.Treasury:
		default:
			panic(fmt.Sprintf("unexpected native contract found: %T", contract))
		}
	}
	return append(newContracts, meta.NewMetadata(neoContract))
}
