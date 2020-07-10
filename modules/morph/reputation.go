package morph

import (
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/peers"
	"github.com/pkg/errors"
	"go.uber.org/dig"
)

type reputationContractResult struct {
	dig.Out

	ReputationContract implementations.MorphReputationContract
}

const (
	reputationContractName = "reputation"

	reputationContractPutOpt = "put_method"

	reputationContractListOpt = "list_method"
)

// ReputationContractPutOptPath returns the config path to put method of Reputation contract.
func ReputationContractPutOptPath() string {
	return optPath(prefix, reputationContractName, reputationContractPutOpt)
}

// ReputationContractListOptPath returns the config path to list method of Reputation contract.
func ReputationContractListOptPath() string {
	return optPath(prefix, reputationContractName, reputationContractListOpt)
}

func newReputationContract(p contractParams, ps peers.Store) (res reputationContractResult, err error) {
	cli, ok := p.MorphContracts[reputationContractName]
	if !ok {
		err = errors.Errorf("missing %s contract client", reputationContractName)
		return
	}

	morphClient := implementations.MorphReputationContract{}
	morphClient.SetReputationContractClient(cli)
	morphClient.SetPublicKeyStore(ps)

	morphClient.SetPutMethodName(
		p.Viper.GetString(
			ReputationContractPutOptPath(),
		),
	)
	morphClient.SetListMethodName(
		p.Viper.GetString(
			ReputationContractListOptPath(),
		),
	)

	res.ReputationContract = morphClient

	return
}
