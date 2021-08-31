package client

import (
	"fmt"
	"strconv"

	nns "github.com/nspcc-dev/neo-go/examples/nft-nd-nns"
	"github.com/nspcc-dev/neo-go/pkg/util"
)

const (
	nnsContractID = 1 // NNS contract must be deployed first in side chain

	// NNSAuditContractName is a name of the audit contract in NNS.
	NNSAuditContractName = "audit.neofs"
	// NNSBalanceContractName is a name of the balance contract in NNS.
	NNSBalanceContractName = "balance.neofs"
	// NNSContainerContractName is a name of the container contract in NNS.
	NNSContainerContractName = "container.neofs"
	// NNSNeoFSIDContractName is a name of the neofsid contract in NNS.
	NNSNeoFSIDContractName = "neofsid.neofs"
	// NNSNetmapContractName is a name of the netmap contract in NNS.
	NNSNetmapContractName = "netmap.neofs"
	// NNSProxyContractName is a name of the proxy contract in NNS.
	NNSProxyContractName = "proxy.neofs"
	// NNSReputationContractName is a name of the reputation contract in NNS.
	NNSReputationContractName = "reputation.neofs"
)

// NNSAlphabetContractName returns contract name of the alphabet contract in NNS
// based on alphabet index.
func NNSAlphabetContractName(index int) string {
	return "alphabet" + strconv.Itoa(index) + ".neofs"
}

// NNSContractAddress returns contract address script hash based on its name
// in NNS contract.
func (c *Client) NNSContractAddress(name string) (sh util.Uint160, err error) {
	if c.multiClient != nil {
		return sh, c.multiClient.iterateClients(func(c *Client) error {
			sh, err = c.NNSContractAddress(name)
			return err
		})
	}

	cs, err := c.client.GetContractStateByID(nnsContractID) // cache it?
	if err != nil {
		return sh, fmt.Errorf("NNS contract state: %w", err)
	}

	s, err := c.client.NNSResolve(cs.Hash, name, nns.TXT)
	if err != nil {
		return sh, fmt.Errorf("NNS.resolve: %w", err)
	}

	sh, err = util.Uint160DecodeStringLE(s)
	if err != nil {
		return sh, fmt.Errorf("NNS u160 decode: %w", err)
	}

	return sh, nil
}
