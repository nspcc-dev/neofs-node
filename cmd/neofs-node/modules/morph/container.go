package morph

import (
	eacl "github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	clientWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/pkg/errors"
	"go.uber.org/dig"
)

type containerContractResult struct {
	dig.Out

	ExtendedACLStore eacl.Storage

	ContainerStorage storage.Storage
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

	var (
		setEACLMethod = p.Viper.GetString(ContainerContractSetEACLOptPath())
		eaclMethod    = p.Viper.GetString(ContainerContractEACLOptPath())
		getMethod     = p.Viper.GetString(ContainerContractGetOptPath())
		putMethod     = p.Viper.GetString(ContainerContractPutOptPath())
		deleteMethod  = p.Viper.GetString(ContainerContractDelOptPath())
		listMethod    = p.Viper.GetString(ContainerContractListOptPath())
	)

	var containerClient *contract.Client
	if containerClient, err = contract.New(client,
		contract.WithSetEACLMethod(setEACLMethod),
		contract.WithEACLMethod(eaclMethod),
		contract.WithGetMethod(getMethod),
		contract.WithPutMethod(putMethod),
		contract.WithDeleteMethod(deleteMethod),
		contract.WithListMethod(listMethod),
	); err != nil {
		return
	}

	var wrapClient *clientWrapper.Wrapper
	if wrapClient, err = clientWrapper.New(containerClient); err != nil {
		return
	}

	res.ContainerStorage = wrapClient
	res.ExtendedACLStore = wrapClient

	return res, nil
}
