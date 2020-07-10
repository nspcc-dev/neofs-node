package morph

import (
	"github.com/nspcc-dev/neofs-node/lib/boot"
	"github.com/nspcc-dev/neofs-node/lib/implementations"
	"github.com/nspcc-dev/neofs-node/lib/ir"
	"github.com/nspcc-dev/neofs-node/lib/netmap"
	"github.com/pkg/errors"
	"go.uber.org/dig"
)

type netmapContractResult struct {
	dig.Out

	NetmapContract *implementations.MorphNetmapContract

	NetMapStorage netmap.Storage

	IRStorage ir.Storage

	StorageBootController boot.StorageBootController
}

const (
	// NetmapContractName is a Netmap contract's config section name.
	NetmapContractName = "netmap"

	netmapContractAddPeerOpt = "add_peer_method"

	netmapContractNewEpochOpt = "new_epoch_method"

	netmapContractNetmapOpt = "netmap_method"

	netmapContractUpdStateOpt = "update_state_method"

	netmapContractIRListOpt = "ir_list_method"
)

// NetmapContractAddPeerOptPath returns the config path to add peer method of Netmap contract.
func NetmapContractAddPeerOptPath() string {
	return optPath(prefix, NetmapContractName, netmapContractAddPeerOpt)
}

// NetmapContractNewEpochOptPath returns the config path to new epoch method of Netmap contract.
func NetmapContractNewEpochOptPath() string {
	return optPath(prefix, NetmapContractName, netmapContractNewEpochOpt)
}

// NetmapContractNetmapOptPath returns the config path to get netmap method of Netmap contract.
func NetmapContractNetmapOptPath() string {
	return optPath(prefix, NetmapContractName, netmapContractNetmapOpt)
}

// NetmapContractUpdateStateOptPath returns the config path to update state method of Netmap contract.
func NetmapContractUpdateStateOptPath() string {
	return optPath(prefix, NetmapContractName, netmapContractUpdStateOpt)
}

// NetmapContractIRListOptPath returns the config path to inner ring list method of Netmap contract.
func NetmapContractIRListOptPath() string {
	return optPath(prefix, NetmapContractName, netmapContractIRListOpt)
}

func newNetmapContract(p contractParams) (res netmapContractResult, err error) {
	client, ok := p.MorphContracts[NetmapContractName]
	if !ok {
		err = errors.Errorf("missing %s contract client", NetmapContractName)
		return
	}

	morphClient := new(implementations.MorphNetmapContract)
	morphClient.SetNetmapContractClient(client)

	morphClient.SetAddPeerMethodName(
		p.Viper.GetString(
			NetmapContractAddPeerOptPath(),
		),
	)
	morphClient.SetNewEpochMethodName(
		p.Viper.GetString(
			NetmapContractNewEpochOptPath(),
		),
	)
	morphClient.SetNetMapMethodName(
		p.Viper.GetString(
			NetmapContractNetmapOptPath(),
		),
	)
	morphClient.SetUpdateStateMethodName(
		p.Viper.GetString(
			NetmapContractUpdateStateOptPath(),
		),
	)
	morphClient.SetIRListMethodName(
		p.Viper.GetString(
			NetmapContractIRListOptPath(),
		),
	)

	bootCtrl := boot.StorageBootController{}
	bootCtrl.SetPeerBootstrapper(morphClient)
	bootCtrl.SetLogger(p.Logger)

	bootPrm := boot.StorageBootParams{}
	bootPrm.SetNodeInfo(&p.NodeInfo)

	bootCtrl.SetBootParams(bootPrm)

	res.StorageBootController = bootCtrl
	res.NetmapContract = morphClient
	res.NetMapStorage = morphClient
	res.IRStorage = morphClient

	return res, nil
}
