package innerring

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/util"
	embeddedcontracts "github.com/nspcc-dev/neofs-contract/contracts"
	"github.com/nspcc-dev/neofs-contract/deploy"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/config"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"go.uber.org/zap"
)

type contracts struct {
	neofs      util.Uint160 // in mainnet
	netmap     util.Uint160 // in FS chain
	balance    util.Uint160 // in FS chain
	container  util.Uint160 // in FS chain
	audit      util.Uint160 // in FS chain
	proxy      util.Uint160 // in FS chain
	processing util.Uint160 // in mainnet
	reputation util.Uint160 // in FS chain

	alphabet []util.Uint160 // in FS chain
}

func initContracts(ctx context.Context, _logger *zap.Logger, cfg *config.Contracts, fsChain *client.Client, withoutMainNet, withoutMainNotary bool) (*contracts, error) {
	var (
		result = new(contracts)
		err    error
	)

	if !withoutMainNet {
		_logger.Debug("decoding configured NeoFS contract...")
		result.neofs, err = util.Uint160DecodeStringLE(cfg.NeoFS)
		if err != nil {
			return nil, fmt.Errorf("can't get neofs script hash: %w", err)
		}

		if !withoutMainNotary {
			_logger.Debug("decoding configured Processing contract...")
			result.processing, err = util.Uint160DecodeStringLE(cfg.Processing)
			if err != nil {
				return nil, fmt.Errorf("can't get processing script hash: %w", err)
			}
		}
	}

	nnsCtx := &nnsContext{Context: ctx}

	targets := [...]struct {
		nnsName string
		dest    *util.Uint160
	}{
		{client.NNSNetmapContractName, &result.netmap},
		{client.NNSBalanceContractName, &result.balance},
		{client.NNSContainerContractName, &result.container},
		{client.NNSAuditContractName, &result.audit},
		{client.NNSReputationContractName, &result.reputation},
		{client.NNSProxyContractName, &result.proxy},
	}

	for _, t := range targets {
		*t.dest, err = parseContract(nnsCtx, _logger, fsChain, t.nnsName)
		if err != nil {
			return nil, fmt.Errorf("can't get %s script hash: %w", t.nnsName, err)
		}
	}

	result.alphabet, err = parseAlphabetContracts(nnsCtx, _logger, fsChain)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func parseAlphabetContracts(ctx *nnsContext, _logger *zap.Logger, fsChain *client.Client) ([]util.Uint160, error) {
	committee, err := fsChain.Committee()
	if err != nil {
		return nil, fmt.Errorf("get FS chain committee: %w", err)
	}
	num := len(committee)

	alpha := make([]util.Uint160, 0, num)

	for ind := range num {
		name := client.NNSAlphabetContractName(ind)
		contractHash, err := parseContract(ctx, _logger, fsChain, name)
		if err != nil {
			if errors.Is(err, client.ErrNNSRecordNotFound) {
				break
			}

			return nil, fmt.Errorf("invalid alphabet %s contract: %w", name, err)
		}

		alpha = append(alpha, contractHash)
	}

	if len(alpha) != int(num) {
		return nil, fmt.Errorf("could not read all contracts: required %d, read %d", num, len(alpha))
	}

	return alpha, nil
}

type nnsContext struct {
	context.Context

	nnsDeployed bool
}

func parseContract(ctx *nnsContext, _logger *zap.Logger, fsChain *client.Client, nnsName string) (res util.Uint160, err error) {
	msPerBlock, err := fsChain.MsPerBlock()
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

			_, err = fsChain.NNSHash()
			if err == nil {
				ctx.nnsDeployed = true
				break
			}

			if !errors.Is(err, neorpc.ErrUnknownContract) {
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

		res, err = fsChain.NNSContractAddress(nnsName)
		if !errors.Is(err, client.ErrNNSRecordNotFound) {
			return
		}

		time.Sleep(pollInterval)
	}
}

func readEmbeddedContracts(deployPrm *deploy.Prm) error {
	cs, err := embeddedcontracts.GetFS()
	if err != nil {
		return fmt.Errorf("read embedded contracts: %w", err)
	}

	mRequired := map[string]*deploy.CommonDeployPrm{
		"NameService":        &deployPrm.NNS.Common,
		"NeoFS Alphabet":     &deployPrm.AlphabetContract.Common,
		"NeoFS Audit":        &deployPrm.AuditContract.Common,
		"NeoFS Balance":      &deployPrm.BalanceContract.Common,
		"NeoFS Container":    &deployPrm.ContainerContract.Common,
		"NeoFS ID":           &deployPrm.NeoFSIDContract.Common,
		"NeoFS Netmap":       &deployPrm.NetmapContract.Common,
		"NeoFS Notary Proxy": &deployPrm.ProxyContract.Common,
		"NeoFS Reputation":   &deployPrm.ReputationContract.Common,
	}

	for i := range cs {
		p, ok := mRequired[cs[i].Manifest.Name]
		if ok {
			p.Manifest = cs[i].Manifest
			p.NEF = cs[i].NEF

			delete(mRequired, cs[i].Manifest.Name)
		}
	}

	if len(mRequired) > 0 {
		missing := make([]string, 0, len(mRequired))
		for name := range mRequired {
			missing = append(missing, name)
		}

		return fmt.Errorf("some contracts are required but not embedded: %v", missing)
	}

	return nil
}
