package neofsid

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/neofsid"
)

// ClientWrapper is a wrapper over NeoFS ID contract
// client which provides convenient methods for
// working with a contract.
//
// Working ClientWrapper must be created via Wrap.
type ClientWrapper neofsid.Client

// NewFromMorph wraps client to work with NeoFS ID contract.
func NewFromMorph(cli *client.Client, contract util.Uint160, fee fixedn.Fixed8) (*ClientWrapper, error) {
	sc, err := client.NewStatic(cli, contract, fee, client.TryNotary())
	if err != nil {
		return nil, fmt.Errorf("could not create client of NeoFS ID contract: %w", err)
	}

	return (*ClientWrapper)(neofsid.New(sc)), nil
}
