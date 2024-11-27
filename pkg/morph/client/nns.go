package client

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
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
)

var (
	// ErrNNSRecordNotFound means that there is no such record in NNS contract.
	ErrNNSRecordNotFound = errors.New("record has not been found in NNS contract")
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
	var conn = c.conn.Load()

	if conn == nil {
		return util.Uint160{}, ErrConnectionLost
	}

	nnsHash, err := c.NNSHash()
	if err != nil {
		return util.Uint160{}, err
	}

	var nnsReader = nns.NewReader(invoker.New(conn.client, nil), nnsHash)

	return nnsReader.ResolveFSContract(name)
}

// NNSHash returns NNS contract hash.
func (c *Client) NNSHash() (util.Uint160, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return util.Uint160{}, ErrConnectionLost
	}

	nnsHash := c.cache.nns()

	if nnsHash == nil {
		h, err := nns.InferHash(conn.client)
		if err != nil {
			return util.Uint160{}, fmt.Errorf("InferHash: %w", err)
		}

		c.cache.setNNSHash(h)
		nnsHash = &h
	}
	return *nnsHash, nil
}

// InitFSChainScope allows to replace [WithAutoFSChainScope] option and
// postpone FS chain scope initialization when NNS contract is not yet ready
// while Client is already needed.
func (c *Client) InitFSChainScope() error {
	var conn = c.conn.Load()

	if conn == nil {
		return ErrConnectionLost
	}

	return autoFSChainScope(conn.client, &c.cfg)
}

func autoFSChainScope(ws *rpcclient.WSClient, conf *cfg) error {
	nnsHash, err := nns.InferHash(ws)
	if err != nil {
		return fmt.Errorf("resolve NNS contract address: %w", err)
	}

	var nnsReader = nns.NewReader(invoker.New(ws, nil), nnsHash)

	balanceHash, err := nnsReader.ResolveFSContract(nns.NameBalance)
	if err != nil {
		return fmt.Errorf("resolve Balance contract address by NNS domain name %q: %w", nns.NameBalance, err)
	}
	cntHash, err := nnsReader.ResolveFSContract(nns.NameContainer)
	if err != nil {
		return fmt.Errorf("resolve Container contract address by NNS domain name %q: %w", nns.NameContainer, err)
	}
	netmapHash, err := nnsReader.ResolveFSContract(nns.NameNetmap)
	if err != nil {
		return fmt.Errorf("resolve Netmap contract address by NNS domain name %q: %w", nns.NameNetmap, err)
	}
	neofsIDHash, err := nnsReader.ResolveFSContract(nns.NameNeoFSID)
	if err != nil {
		return fmt.Errorf("resolve NeoFS ID contract address by NNS domain name %q: %w", nns.NameNeoFSID, err)
	}

	conf.signer = GetUniversalSignerScope(nnsHash, balanceHash, cntHash, netmapHash, neofsIDHash)
	return nil
}

// GetUniversalSignerScope returns a universal (applicable for any valid NeoFS
// contract call) scope that should be used by IR and SNs. It contains a set of
// Rules for contracts calling each other and a regular CalledByEntry permission.
func GetUniversalSignerScope(nnsHash, balanceHash, cntHash, netmapHash, neofsIDHash util.Uint160) *transaction.Signer {
	return &transaction.Signer{
		Scopes: transaction.CalledByEntry | transaction.Rules,
		Rules: []transaction.WitnessRule{{
			Action: transaction.WitnessAllow,
			Condition: &transaction.ConditionAnd{
				(*transaction.ConditionCalledByContract)(&cntHash),
				(*transaction.ConditionScriptHash)(&balanceHash),
			},
		}, {
			Action: transaction.WitnessAllow,
			Condition: &transaction.ConditionAnd{
				(*transaction.ConditionCalledByContract)(&cntHash),
				(*transaction.ConditionScriptHash)(&nnsHash),
			},
		}, {
			Action: transaction.WitnessAllow,
			Condition: &transaction.ConditionAnd{
				(*transaction.ConditionCalledByContract)(&cntHash),
				(*transaction.ConditionScriptHash)(&neofsIDHash),
			},
		}, {
			Action: transaction.WitnessAllow,
			Condition: &transaction.ConditionAnd{
				(*transaction.ConditionCalledByContract)(&netmapHash),
				(*transaction.ConditionScriptHash)(&cntHash),
			},
		}, {
			Action: transaction.WitnessAllow,
			Condition: &transaction.ConditionAnd{
				(*transaction.ConditionCalledByContract)(&netmapHash),
				(*transaction.ConditionScriptHash)(&balanceHash),
			},
		}},
	}
}
