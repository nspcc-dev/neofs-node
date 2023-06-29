package innerring

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"go.uber.org/zap"
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
	neofsID    util.Uint160 // in morph

	alphabet alphabetContracts // in morph
}

func initContracts(ctx context.Context, _logger *logger.Logger, cfg *viper.Viper, morph *client.Client, withoutMainNet, withoutMainNotary bool) (*contracts, error) {
	var (
		result = new(contracts)
		err    error
	)

	if !withoutMainNet {
		_logger.Debug("decoding configured NeoFS contract...")
		result.neofs, err = util.Uint160DecodeStringLE(cfg.GetString("contracts.neofs"))
		if err != nil {
			return nil, fmt.Errorf("can't get neofs script hash: %w", err)
		}

		if !withoutMainNotary {
			_logger.Debug("decoding configured Processing contract...")
			result.processing, err = util.Uint160DecodeStringLE(cfg.GetString("contracts.processing"))
			if err != nil {
				return nil, fmt.Errorf("can't get processing script hash: %w", err)
			}
		}
	}

	nnsCtx := &nnsContext{Context: ctx}

	result.proxy, err = parseContract(nnsCtx, _logger, cfg, morph, "contracts.proxy", client.NNSProxyContractName)
	if err != nil {
		return nil, fmt.Errorf("can't get proxy script hash: %w", err)
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
		{"contracts.neofsid", client.NNSNeoFSIDContractName, &result.neofsID},
	}

	for _, t := range targets {
		*t.dest, err = parseContract(nnsCtx, _logger, cfg, morph, t.cfgName, t.nnsName)
		if err != nil {
			name := strings.TrimPrefix(t.cfgName, "contracts.")
			return nil, fmt.Errorf("can't get %s script hash: %w", name, err)
		}
	}

	result.alphabet, err = parseAlphabetContracts(nnsCtx, _logger, cfg, morph)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func parseAlphabetContracts(ctx *nnsContext, _logger *logger.Logger, cfg *viper.Viper, morph *client.Client) (alphabetContracts, error) {
	var num GlagoliticLetter
	const numConfigKey = "contracts.alphabet.amount"
	if cfg.IsSet(numConfigKey) {
		u, err := cast.ToUintE(cfg.Get(numConfigKey))
		if err != nil {
			return nil, fmt.Errorf("invalid config '%s': %w", numConfigKey, err)
		}
		num = GlagoliticLetter(u)
	} else {
		committee, err := morph.Committee()
		if err != nil {
			return nil, fmt.Errorf("get Sidechain committee: %w", err)
		}
		num = GlagoliticLetter(len(committee))
	}

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
		contractHash, err := parseContract(ctx, _logger, cfg, morph,
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

type nnsContext struct {
	context.Context

	nnsDeployed bool
}

func parseContract(ctx *nnsContext, _logger *logger.Logger, cfg *viper.Viper, morph *client.Client, cfgName, nnsName string) (res util.Uint160, err error) {
	if cfg.IsSet(cfgName) {
		_logger.Debug("decoding configured contract...", zap.String("config key", cfgName))
		return util.Uint160DecodeStringLE(cfg.GetString(cfgName))
	}

	msPerBlock, err := morph.MsPerBlock()
	if err != nil {
		return res, fmt.Errorf("get ms per block protocol config: %w", err)
	}

	pollInterval := time.Duration(msPerBlock) * time.Millisecond

	if !ctx.nnsDeployed {
		for {
			_logger.Info("waiting for NNS contract to be deployed...")

			select {
			case <-ctx.Done():
				return res, fmt.Errorf("waiting for NNS contract: %w", ctx.Err())
			default:
			}

			_, err = morph.NNSHash()
			if err == nil {
				ctx.nnsDeployed = true
				break
			}

			// FIXME: unstable approach, but currently there is no other way
			//  Track neofs-node#2285
			if !strings.Contains(err.Error(), "Unknown contract") {
				return
			}

			time.Sleep(pollInterval)
		}
	}

	for {
		_logger.Info("waiting for contract registration in the NNS...", zap.String("name", nnsName))

		select {
		case <-ctx.Done():
			return res, fmt.Errorf("waiting for contract: %w", ctx.Err())
		default:
		}

		res, err = morph.NNSContractAddress(nnsName)
		if !errors.Is(err, client.ErrNNSRecordNotFound) {
			return
		}

		time.Sleep(pollInterval)
	}
}
