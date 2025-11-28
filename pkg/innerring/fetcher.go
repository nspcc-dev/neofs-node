package innerring

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// NewIRFetcherWithNotary creates IrFetcherWithNotary.
//
// IrFetcherWithNotary can be used to obtain innerring key list if
// network that client is connected to supports notary contract.
//
// Passed client is required. Panics if nil.
func NewIRFetcherWithNotary(cli *client.Client) *IrFetcherWithNotary {
	if cli == nil {
		panic("could not init IRFetcher with notary: client must not be nil")
	}
	return &IrFetcherWithNotary{cli: cli}
}

// IrFetcherWithNotary fetches keys using notary contract. Must be created
// with NewIRFetcherWithNotary.
type IrFetcherWithNotary struct {
	cli *client.Client
}

// InnerRingKeys fetches list of innerring keys from NeoFSAlphabet
// role in FS chain.
func (fN IrFetcherWithNotary) InnerRingKeys() (keys.PublicKeys, error) {
	return fN.cli.NeoFSAlphabetList()
}
