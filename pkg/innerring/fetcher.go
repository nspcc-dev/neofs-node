package innerring

import (
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	nmClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
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

// NewIRFetcherWithoutNotary creates IrFetcherWithoutNotary.
//
// IrFetcherWithoutNotary must be used to obtain innerring key list if
// network that netmap wrapper is connected to does not support notary
// contract.
//
// Passed netmap wrapper is required. Panics if nil.
func NewIRFetcherWithoutNotary(nm *nmClient.Client) *IrFetcherWithoutNotary {
	if nm == nil {
		panic("could not init IRFetcher without notary: netmap wrapper must not be nil")
	}
	return &IrFetcherWithoutNotary{nm: nm}
}

// IrFetcherWithNotary fetches keys using notary contract. Must be created
// with NewIRFetcherWithNotary.
type IrFetcherWithNotary struct {
	cli *client.Client
}

// IrFetcherWithoutNotary fetches keys using netmap contract. Must be created
// with NewIRFetcherWithoutNotary.
type IrFetcherWithoutNotary struct {
	nm *nmClient.Client
}

// InnerRingKeys fetches list of innerring keys from NeoFSAlphabet
// role in FS chain.
func (fN IrFetcherWithNotary) InnerRingKeys() (keys.PublicKeys, error) {
	return fN.cli.NeoFSAlphabetList()
}

// InnerRingKeys fetches list of innerring keys from netmap contract
// in FS chain.
func (f IrFetcherWithoutNotary) InnerRingKeys() (keys.PublicKeys, error) {
	return f.nm.GetInnerRingList()
}
