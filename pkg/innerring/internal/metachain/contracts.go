package metachain

import (
	neogoconfig "github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/interop"
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/gas"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/metachain/meta"
)

// NewCustomNatives returns custom list of native contracts for metadata
// side chain. Returned contracts:
//   - Management
//   - Ledger
//   - NEO
//   - redefined GAS (see [gas.NewGAS] for details)
//   - Policy
//   - Designate
//   - Notary
//   - new native metadata contract (see [meta.NewMetadata] for details).
func NewCustomNatives(cfg neogoconfig.ProtocolConfiguration) []interop.Contract {
	mgmt := native.NewManagement()
	ledger := native.NewLedger()

	g := gas.NewGAS()
	n := native.NewNEO(cfg)
	p := native.NewPolicy()

	n.GAS = g
	n.Policy = p

	mgmt.NEO = n
	mgmt.Policy = p
	ledger.Policy = p

	desig := native.NewDesignate(cfg.Genesis.Roles)
	desig.NEO = n

	notary := native.NewNotary()
	notary.Policy = p
	notary.GAS = g
	notary.NEO = n
	notary.Desig = desig

	return []interop.Contract{
		mgmt,
		ledger,
		n,
		g,
		p,
		desig,
		notary,
		meta.NewMetadata(n),
	}
}
