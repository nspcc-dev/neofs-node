package client

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
)

const (
	// NNSAuditContractName is a name of the audit contract in NNS.
	NNSAuditContractName = nns.NameAudit
	// NNSBalanceContractName is a name of the balance contract in NNS.
	NNSBalanceContractName = nns.NameBalance
	// NNSContainerContractName is a name of the container contract in NNS.
	NNSContainerContractName = nns.NameContainer
	// NNSNeoFSIDContractName is a name of the neofsid contract in NNS.
	NNSNeoFSIDContractName = nns.NameNeoFSID
	// NNSNetmapContractName is a name of the netmap contract in NNS.
	NNSNetmapContractName = nns.NameNetmap
	// NNSProxyContractName is a name of the proxy contract in NNS.
	NNSProxyContractName = nns.NameProxy
	// NNSReputationContractName is a name of the reputation contract in NNS.
	NNSReputationContractName = nns.NameReputation
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
	return nns.NameAlphabetPrefix + strconv.Itoa(index)
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

	var nnsReader = nns.NewReader(invoker.New(c.client, nil), nnsHash)

	return nnsReader.ResolveFSContract(name)
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
		h, err := nns.InferHash(c.client)
		if err != nil {
			return util.Uint160{}, fmt.Errorf("InferHash: %w", err)
		}

		c.cache.setNNSHash(h)
		nnsHash = &h
	}
	return *nnsHash, nil
}

func nnsResolveItems(c *rpcclient.WSClient, nnsHash util.Uint160, domain string) ([]string, error) {
	var nnsReader = nns.NewReader(invoker.New(c, nil), nnsHash)

	found, err := exists(nnsReader, domain)
	if err != nil {
		return nil, fmt.Errorf("could not check presence in NNS contract for %s: %w", domain, err)
	}

	if !found {
		return nil, ErrNNSRecordNotFound
	}

	result, err := nnsReader.Resolve(domain, nns.TXT)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, errEmptyResultStack
	}
	return result, nil
}

// exists checks domain presence in the NNS contract with given address by
// calling `ownerOf` method. Returns true if call succeeded only.
func exists(nnsReader *nns.ContractReader, domain string) (bool, error) {
	_, err := nnsReader.OwnerOf([]byte(domain))

	if err != nil {
		if strings.Contains(err.Error(), "invocation failed") {
			err = nil
		}
		return false, err
	}

	return true, nil
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

	items, err := nnsResolveItems(c.client, nnsHash, NNSGroupKeyName)
	if err != nil {
		return nil, err
	}

	pub, err := keys.NewPublicKeyFromString(string(items[0]))
	if err != nil {
		return nil, err
	}

	c.cache.setGroupKey(pub)
	return pub, nil
}
