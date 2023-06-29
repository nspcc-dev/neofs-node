package client

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-contract/nns"
)

const (
	nnsContractID = 1 // NNS contract must be deployed first in the sidechain

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
	// NNSGroupKeyName is a name for the NeoFS group key record in NNS.
	NNSGroupKeyName = "group.neofs"
)

var (
	// ErrNNSRecordNotFound means that there is no such record in NNS contract.
	ErrNNSRecordNotFound = errors.New("record has not been found in NNS contract")

	errEmptyResultStack = errors.New("returned result stack is empty")
)

// NNSAlphabetContractName returns contract name of the alphabet contract in NNS
// based on alphabet index.
func NNSAlphabetContractName(index int) string {
	return "alphabet" + strconv.Itoa(index) + ".neofs"
}

// NNSContractAddress returns contract address script hash based on its name
// in NNS contract.
// If script hash has not been found, returns ErrNNSRecordNotFound.
func (c *Client) NNSContractAddress(name string) (sh util.Uint160, err error) {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return util.Uint160{}, ErrConnectionLost
	}

	nnsHash, err := c.NNSHash()
	if err != nil {
		return util.Uint160{}, err
	}

	sh, err = nnsResolve(c.client, nnsHash, name)
	if err != nil {
		return sh, fmt.Errorf("NNS.resolve: %w", err)
	}
	return sh, nil
}

// NNSHash returns NNS contract hash.
func (c *Client) NNSHash() (util.Uint160, error) {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return util.Uint160{}, ErrConnectionLost
	}

	nnsHash := c.cache.nns()

	if nnsHash == nil {
		cs, err := c.client.GetContractStateByID(nnsContractID)
		if err != nil {
			return util.Uint160{}, fmt.Errorf("NNS contract state: %w", err)
		}

		c.cache.setNNSHash(cs.Hash)
		nnsHash = &cs.Hash
	}
	return *nnsHash, nil
}

func nnsResolveItem(c *rpcclient.WSClient, nnsHash util.Uint160, domain string) (stackitem.Item, error) {
	found, err := exists(c, nnsHash, domain)
	if err != nil {
		return nil, fmt.Errorf("could not check presence in NNS contract for %s: %w", domain, err)
	}

	if !found {
		return nil, ErrNNSRecordNotFound
	}

	result, err := c.InvokeFunction(nnsHash, "resolve", []smartcontract.Parameter{
		{
			Type:  smartcontract.StringType,
			Value: domain,
		},
		{
			Type:  smartcontract.IntegerType,
			Value: big.NewInt(int64(nns.TXT)),
		},
	}, nil)
	if err != nil {
		return nil, err
	}
	if result.State != vmstate.Halt.String() {
		return nil, fmt.Errorf("invocation failed: %s", result.FaultException)
	}
	if len(result.Stack) == 0 {
		return nil, errEmptyResultStack
	}
	return result.Stack[0], nil
}

func nnsResolve(c *rpcclient.WSClient, nnsHash util.Uint160, domain string) (util.Uint160, error) {
	res, err := nnsResolveItem(c, nnsHash, domain)
	if err != nil {
		return util.Uint160{}, err
	}

	// Parse the result of resolving NNS record.
	// It works with multiple formats (corresponding to multiple NNS versions).
	// If array of hashes is provided, it returns only the first one.
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

	// We support several formats for hash encoding, this logic should be maintained in sync
	// with parseNNSResolveResult from cmd/neofs-adm/internal/modules/morph/initialize_nns.go
	h, err := util.Uint160DecodeStringLE(string(bs))
	if err == nil {
		return h, nil
	}

	h, err = address.StringToUint160(string(bs))
	if err == nil {
		return h, nil
	}

	return util.Uint160{}, errors.New("no valid hashes are found")
}

// exists checks domain presence in the NNS contract with given address by
// calling `ownerOf` method. Returns true if call succeeded only.
func exists(c *rpcclient.WSClient, nnsHash util.Uint160, domain string) (bool, error) {
	result, err := c.InvokeFunction(nnsHash, "ownerOf", []smartcontract.Parameter{
		{
			Type:  smartcontract.ByteArrayType,
			Value: []byte(domain),
		},
	}, nil)
	if err != nil {
		return false, err
	}
	return result.State == vmstate.Halt.String(), nil
}

// SetGroupSignerScope makes the default signer scope include all NeoFS contracts.
// Should be called for side-chain client only.
func (c *Client) SetGroupSignerScope() error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	pub, err := c.contractGroupKey()
	if err != nil {
		return err
	}

	// Don't change c before everything is OK.
	cfg := c.cfg
	cfg.signer = &transaction.Signer{
		Scopes:        transaction.CustomGroups | transaction.CalledByEntry,
		AllowedGroups: []*keys.PublicKey{pub},
	}
	rpcActor, err := newActor(c.client, c.acc, cfg)
	if err != nil {
		return err
	}
	c.cfg = cfg
	c.setActor(rpcActor)
	return nil
}

// contractGroupKey returns public key designating NeoFS contract group.
func (c *Client) contractGroupKey() (*keys.PublicKey, error) {
	if gKey := c.cache.groupKey(); gKey != nil {
		return gKey, nil
	}

	nnsHash, err := c.NNSHash()
	if err != nil {
		return nil, err
	}

	item, err := nnsResolveItem(c.client, nnsHash, NNSGroupKeyName)
	if err != nil {
		return nil, err
	}

	arr, ok := item.Value().([]stackitem.Item)
	if !ok || len(arr) == 0 {
		return nil, errors.New("NNS record is missing")
	}

	bs, err := arr[0].TryBytes()
	if err != nil {
		return nil, err
	}

	pub, err := keys.NewPublicKeyFromString(string(bs))
	if err != nil {
		return nil, err
	}

	c.cache.setGroupKey(pub)
	return pub, nil
}
