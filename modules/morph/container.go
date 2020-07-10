package morph

import (
	"github.com/nspcc-dev/neofs-node/lib/acl"
	"github.com/nspcc-dev/neofs-node/lib/container"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/pkg/errors"
	"go.uber.org/dig"
)

type containerContractResult struct {
	dig.Out

	ContainerContract *implementations.MorphContainerContract

	BinaryExtendedACLStore acl.BinaryExtendedACLStore

	ExtendedACLSource acl.ExtendedACLSource

	ContainerStorage container.Storage
}

const (
	containerContractName = "container"

	containerContractSetEACLOpt = "set_eacl_method"

	containerContractEACLOpt = "get_eacl_method"

	containerContractPutOpt = "put_method"

	containerContractGetOpt = "get_method"

	containerContractDelOpt = "delete_method"

	containerContractListOpt = "list_method"
)

// ContainerContractSetEACLOptPath returns the config path to set eACL method name of Container contract.
func ContainerContractSetEACLOptPath() string {
	return optPath(prefix, containerContractName, containerContractSetEACLOpt)
}

// ContainerContractEACLOptPath returns the config path to get eACL method name of Container contract.
func ContainerContractEACLOptPath() string {
	return optPath(prefix, containerContractName, containerContractEACLOpt)
}

// ContainerContractPutOptPath returns the config path to put container method name of Container contract.
func ContainerContractPutOptPath() string {
	return optPath(prefix, containerContractName, containerContractPutOpt)
}

// ContainerContractGetOptPath returns the config path to get container method name of Container contract.
func ContainerContractGetOptPath() string {
	return optPath(prefix, containerContractName, containerContractGetOpt)
}

// ContainerContractDelOptPath returns the config path to delete container method name of Container contract.
func ContainerContractDelOptPath() string {
	return optPath(prefix, containerContractName, containerContractDelOpt)
}

// ContainerContractListOptPath returns the config path to list containers method name of Container contract.
func ContainerContractListOptPath() string {
	return optPath(prefix, containerContractName, containerContractListOpt)
}

func newContainerContract(p contractParams) (res containerContractResult, err error) {
	client, ok := p.MorphContracts[containerContractName]
	if !ok {
		err = errors.Errorf("missing %s contract client", containerContractName)
		return
	}

	morphClient := new(implementations.MorphContainerContract)
	morphClient.SetContainerContractClient(client)

	morphClient.SetEACLSetMethodName(
		p.Viper.GetString(
			ContainerContractSetEACLOptPath(),
		),
	)
	morphClient.SetEACLGetMethodName(
		p.Viper.GetString(
			ContainerContractEACLOptPath(),
		),
	)
	morphClient.SetContainerGetMethodName(
		p.Viper.GetString(
			ContainerContractGetOptPath(),
		),
	)
	morphClient.SetContainerPutMethodName(
		p.Viper.GetString(
			ContainerContractPutOptPath(),
		),
	)
	morphClient.SetContainerDeleteMethodName(
		p.Viper.GetString(
			ContainerContractDelOptPath(),
		),
	)
	morphClient.SetContainerListMethodName(
		p.Viper.GetString(
			ContainerContractListOptPath(),
		),
	)

	res.ContainerContract = morphClient

	res.BinaryExtendedACLStore = morphClient

	res.ExtendedACLSource, err = implementations.ExtendedACLSourceFromBinary(res.BinaryExtendedACLStore)
	if err != nil {
		return
	}

	res.ContainerStorage = morphClient

	return res, nil
}
