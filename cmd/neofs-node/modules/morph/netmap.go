package morph

import (
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	clientWrapper "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/network/bootstrap"
	"github.com/pkg/errors"
	"go.uber.org/dig"
)

type netmapContractResult struct {
	dig.Out

	Client *clientWrapper.Wrapper

	NodeRegisterer *bootstrap.Registerer
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

	var (
		addPeerMethod  = p.Viper.GetString(NetmapContractAddPeerOptPath())
		newEpochMethod = p.Viper.GetString(NetmapContractNewEpochOptPath())
		netmapMethod   = p.Viper.GetString(NetmapContractNetmapOptPath())
		updStateMethod = p.Viper.GetString(NetmapContractUpdateStateOptPath())
		irListMethod   = p.Viper.GetString(NetmapContractIRListOptPath())
	)

	var c *contract.Client
	if c, err = contract.New(client,
		contract.WithAddPeerMethod(addPeerMethod),
		contract.WithNewEpochMethod(newEpochMethod),
		contract.WithNetMapMethod(netmapMethod),
		contract.WithUpdateStateMethod(updStateMethod),
		contract.WithInnerRingListMethod(irListMethod),
	); err != nil {
		return
	}

	if res.Client, err = clientWrapper.New(c); err != nil {
		return
	}

	if res.NodeRegisterer, err = bootstrap.New(res.Client, p.NodeInfo); err != nil {
		return
	}

	return res, nil
}
