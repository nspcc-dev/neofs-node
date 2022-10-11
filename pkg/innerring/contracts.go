package innerring

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/spf13/viper"
)

// contractName is an enumeration of names of the smart contracts deployed in
// the NeoFS blockchain network.
type contractName string

// contractSuffix is a common name suffix of the smart contract deployed in the
// NeoFS Sidechain. Names are registered in the NeoFS blockchain network using
// NNS.
const contractSuffix = ".neofs"

// Named NeoFS contracts.
const (
	contractNeoFS      contractName = "NeoFS"
	contractProcessing contractName = "Processing"
	contractContainer  contractName = "container" + contractSuffix
	contractAudit      contractName = "audit" + contractSuffix
	contractNetmap     contractName = "netmap" + contractSuffix
	contractBalance    contractName = "balance" + contractSuffix
	contractNeoFSID    contractName = "neofsid" + contractSuffix
	contractProxy      contractName = "proxy" + contractSuffix
	contractReputation contractName = "reputation" + contractSuffix
	contractSubnet     contractName = "subnet" + contractSuffix
)

func (x contractName) String() string {
	return string(x)
}

// alphabetContractNamePrefix is a name prefix of the NeoFS Alphabet contracts
// deployed in the NeoFS Sidechain. Names are registered in the NeoFS blockchain
// network using NNS.
const alphabetContractNamePrefix = "alphabet"

// alphabetContractName returns contractName for the NeoFS Alphabet contract
// associated with the given GlagoliticLetter. Name format is 'alphabetI.neofs' where
// 'I' is an index of the GlagoliticLetter in the Glagolitsa alphabet.
func alphabetContractName(l GlagoliticLetter) contractName {
	if l < az || l >= lastLetterNum {
		panic(fmt.Sprintf("invalid glagolitic letter enum value %v", l))
	}

	return contractName(alphabetContractNamePrefix + strconv.Itoa(int(l)) + contractSuffix)
}

// contracts represents table of the smart contracts deployed in the NeoFS
// blockchain network.
type contracts struct {
	m map[contractName]util.Uint160
}

// newContracts creates new contracts instance and fills it with pre-configured
// values stored in the provided viper.Viper container. Mapping between NeoFS
// contract names and configuration paths is controlled by the related
// parameter. The newContracts returns an error if any of the contracts
// mentioned in the map is missing or is not a little-endian hex-encoded string.
func newContracts(cfg *viper.Viper, m map[contractName]string) (*contracts, error) {
	var c contracts
	c.m = make(map[contractName]util.Uint160, 10)

	for name, cfgPath := range m {
		if !cfg.IsSet(cfgPath) {
			return nil, fmt.Errorf("contract '%s' address is not configured in path '%s'", name, cfgPath)
		}

		contract, err := util.Uint160DecodeStringLE(cfg.GetString(cfgPath))
		if err != nil {
			return nil, fmt.Errorf("invalid contract '%s' little-endian hex string '%s': %w", name, cfgPath, err)
		}

		c.set(name, contract)
	}

	return &c, nil
}

// set writes address of the NeoFS smart contract by its name to the contracts' table.
func (x *contracts) set(name contractName, addr util.Uint160) {
	x.m[name] = addr
}

// get reads address of the NeoFS smart contract by its contractName from the
// contracts' table. Return zero value if there is no table record for the
// requested contractName.
func (x *contracts) get(name contractName) util.Uint160 {
	return x.m[name]
}

// resolve attempts to resolve NeoFS contractName from its address. Returns
// false if there is no table record with the requested address.
func (x *contracts) resolve(contract util.Uint160) (contractName, bool) {
	for k, v := range x.m {
		if v.Equals(contract) {
			return k, true
		}
	}

	return "", false
}

// getAlphabetContract is a wrapper over contracts.get which simplifies reading
// address of the NeoFS Alphabet contract by given GlagoliticLetter.
func getAlphabetContract(c *contracts, l GlagoliticLetter) util.Uint160 {
	return c.get(alphabetContractName(l))
}

// iterateAlphabet iterates over all NeoFS Alphabet contract-related records and
// passes corresponding GlagoliticLetter and address to the given handler.
func (x *contracts) iterateAlphabet(f func(letter GlagoliticLetter, contract util.Uint160)) {
	var letter GlagoliticLetter

	for k, v := range x.m {
		s := strings.TrimPrefix(string(k), alphabetContractNamePrefix)
		if s != string(k) {
			_, err := fmt.Sscan(s, &letter)
			if err != nil {
				panic(fmt.Sprintf("decode Alphabet contract name string: %v", err))
			}

			f(letter, v)
		}
	}
}
