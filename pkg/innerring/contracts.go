package innerring

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/spf13/viper"
)

type contracts struct {
	neofs      util.Uint160 // in mainnet
	netmap     util.Uint160 // in morph
	balance    util.Uint160 // in morph
	container  util.Uint160 // in morph
	audit      util.Uint160 // in morph
	proxy      util.Uint160 // in morph
	processing util.Uint160 // in mainnet
	reputation util.Uint160 // in morph
	subnet     util.Uint160 // in morph
	neofsID    util.Uint160 // in morph

	alphabet alphabetContracts // in morph
}

func parseContracts(cfg *viper.Viper, morph *client.Client, withoutMainNet, withoutMainNotary, withoutSideNotary bool) (*contracts, error) {
	var (
		result = new(contracts)
		err    error
	)

	if !withoutMainNet {
		result.neofs, err = util.Uint160DecodeStringLE(cfg.GetString("contracts.neofs"))
		if err != nil {
			return nil, fmt.Errorf("can't get neofs script hash: %w", err)
		}

		if !withoutMainNotary {
			result.processing, err = util.Uint160DecodeStringLE(cfg.GetString("contracts.processing"))
			if err != nil {
				return nil, fmt.Errorf("can't get processing script hash: %w", err)
			}
		}
	}

	if !withoutSideNotary {
		result.proxy, err = parseContract(cfg, morph, "contracts.proxy", client.NNSProxyContractName)
		if err != nil {
			return nil, fmt.Errorf("can't get proxy script hash: %w", err)
		}
	}

	targets := [...]struct {
		cfgName string
		nnsName string
		dest    *util.Uint160
	}{
		{"contracts.netmap", client.NNSNetmapContractName, &result.netmap},
		{"contracts.balance", client.NNSBalanceContractName, &result.balance},
		{"contracts.container", client.NNSContainerContractName, &result.container},
		{"contracts.audit", client.NNSAuditContractName, &result.audit},
		{"contracts.reputation", client.NNSReputationContractName, &result.reputation},
		{"contracts.subnet", client.NNSSubnetworkContractName, &result.subnet},
		{"contracts.neofsid", client.NNSNeoFSIDContractName, &result.neofsID},
	}

	for _, t := range targets {
		*t.dest, err = parseContract(cfg, morph, t.cfgName, t.nnsName)
		if err != nil {
			name := strings.TrimPrefix(t.cfgName, "contracts.")
			return nil, fmt.Errorf("can't get %s script hash: %w", name, err)
		}
	}

	result.alphabet, err = parseAlphabetContracts(cfg, morph)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func parseAlphabetContracts(cfg *viper.Viper, morph *client.Client) (alphabetContracts, error) {
	num := GlagoliticLetter(cfg.GetUint("contracts.alphabet.amount"))
	alpha := newAlphabetContracts()

	if num > lastLetterNum {
		return nil, fmt.Errorf("amount of alphabet contracts overflows glagolitsa %d > %d", num, lastLetterNum)
	}

	thresholdIsSet := num != 0

	if !thresholdIsSet {
		// try to read maximum alphabet contracts
		// if threshold has not been set manually
		num = lastLetterNum
	}

	for letter := az; letter < num; letter++ {
		contractHash, err := parseContract(cfg, morph,
			"contracts.alphabet."+letter.String(),
			client.NNSAlphabetContractName(int(letter)),
		)
		if err != nil {
			if errors.Is(err, client.ErrNNSRecordNotFound) {
				break
			}

			return nil, fmt.Errorf("invalid alphabet %s contract: %w", letter, err)
		}

		alpha.set(letter, contractHash)
	}

	if thresholdIsSet && len(alpha) != int(num) {
		return nil, fmt.Errorf("could not read all contracts: required %d, read %d", num, len(alpha))
	}

	return alpha, nil
}

func parseContract(cfg *viper.Viper, morph *client.Client, cfgName, nnsName string) (res util.Uint160, err error) {
	contractStr := cfg.GetString(cfgName)
	if len(contractStr) == 0 {
		return morph.NNSContractAddress(nnsName)
	}

	return util.Uint160DecodeStringLE(contractStr)
}
