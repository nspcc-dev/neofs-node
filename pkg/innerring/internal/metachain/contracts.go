package metachain

import (
	"fmt"
	"maps"
	"slices"

	neogoconfig "github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/gas"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/meta"
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

	var requiredContracts = map[string]struct{}{
		nativenames.Gas:         {},
		nativenames.Neo:         {},
		nativenames.Management:  {},
		nativenames.Ledger:      {},
		nativenames.Policy:      {},
		nativenames.Designation: {},
		nativenames.Notary:      {},
	}

	for _, contract := range defaultContracts {
		delete(requiredContracts, contract.Metadata().Name)

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

	if len(requiredContracts) != 0 {
		panic(fmt.Sprintf("not found required default native contracts: %v", slices.Collect(maps.Keys(requiredContracts))))
	}

	return append(newContracts, meta.NewMetadata(neoContract))
}
