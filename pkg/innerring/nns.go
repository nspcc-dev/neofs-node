package innerring

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep11"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/util"
	nnsrpc "github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/privatedomains"
)

// provides services of the NeoFS Name Service consumed by the Inner Ring node.
type neoFSNNS struct {
	invoker  nep11.Invoker
	contract *nnsrpc.ContractReader
}

// creates NeoFS Name Service provider working with the Neo smart contract
// deployed in the Neo network accessed through the specified [nep11.Invoker].
func newNeoFSNNS(contractAddress util.Uint160, contractCaller nep11.Invoker) *neoFSNNS {
	return &neoFSNNS{
		invoker:  contractCaller,
		contract: nnsrpc.NewReader(contractCaller, contractAddress),
	}
}

var errDomainNotFound = errors.New("domain not found")

// CheckDomainRecord calls iterating 'getAllRecords' method of the parameterized
// Neo smart contract passing the given domain name. If contract throws 'token
// not found' exception, CheckDomainRecord returns errDomainNotFound. Each value
// in the resulting iterator is expected to be structure with at least 3 fields.
// If any value has the 2nd field is a number equal to 16 (TXT record type in
// the NNS) and the 3rd one is a string equal to the specified record,
// CheckDomainRecord returns nil. Otherwise,
// [privatedomains.ErrMissingDomainRecord] is returned.
func (x *neoFSNNS) CheckDomainRecord(domain string, record string) error {
	records, err := x.contract.Resolve(domain, nnsrpc.TXT)
	if err != nil {
		var ex unwrap.Exception
		if errors.As(err, &ex) && strings.Contains(string(ex), "token not found") {
			return errDomainNotFound
		}

		return fmt.Errorf("get all text records of the NNS domain %q: %w", domain, err)
	}

	if slices.Contains(records, record) {
		return nil
	}

	return privatedomains.ErrMissingDomainRecord
}
