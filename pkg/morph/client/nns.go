package client

import (
	"errors"
	"fmt"
	"strconv"

	nns "github.com/nspcc-dev/neo-go/examples/nft-nd-nns"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
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

	sh, err = nnsResolve(c.client, cs.Hash, name)
	if err != nil {
		return sh, fmt.Errorf("NNS.resolve: %w", err)
	}
	return sh, nil
}

func nnsResolve(c *client.Client, nnsHash util.Uint160, domain string) (util.Uint160, error) {
	result, err := c.InvokeFunction(nnsHash, "resolve", []smartcontract.Parameter{
		{
			Type:  smartcontract.StringType,
			Value: domain,
		},
		{
			Type:  smartcontract.IntegerType,
			Value: int64(nns.TXT),
		},
	}, nil)
	if err != nil {
		return util.Uint160{}, err
	}
	if result.State != vm.HaltState.String() {
		return util.Uint160{}, fmt.Errorf("invocation failed: %s", result.FaultException)
	}
	if len(result.Stack) == 0 {
		return util.Uint160{}, errors.New("result stack is empty")
	}

	// Parse the result of resolving NNS record.
	// It works with multiple formats (corresponding to multiple NNS versions).
	// If array of hashes is provided, it returns only the first one.
	res := result.Stack[0]
	if arr, ok := res.Value().([]stackitem.Item); ok {
		if len(arr) == 0 {
			return util.Uint160{}, errors.New("NNS record is missing")
		}
		res = arr[0]
	}
	bs, err := res.TryBytes()
	if err != nil {
		return util.Uint160{}, fmt.Errorf("malformed response: %w", err)
	}
	return util.Uint160DecodeStringLE(string(bs))
}
