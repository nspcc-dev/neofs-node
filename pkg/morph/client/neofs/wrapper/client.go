package neofscontract

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
)

// ClientWrapper is a wrapper over NeoFS contract
// client which provides convenient methods for
// working with a contract.
//
// Working ClientWrapper must be created via NewFromMorph.
type ClientWrapper neofscontract.Client

// NewFromMorph wraps client to work with NeoFS contract.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8) (*ClientWrapper, error) {
	sc, err := client.NewStatic(cli, contract, fee, client.TryNotary())
	if err != nil {
		return nil, fmt.Errorf("could not create client of NeoFS contract: %w", err)
	}

	return (*ClientWrapper)(neofscontract.New(sc)), nil
}
