package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

// NetMap represents the NeoFS network map.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/NetMap.
type NetMap = netmap.NetMap

// Info represents node information.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/netmap.Info.
type Info = netmap.Info

// GetNetMap receives information list about storage nodes
// through the Netmap contract call, composes network map
// from them and returns it.
func (w *Wrapper) GetNetMap() (*NetMap, error) {
	// prepare invocation arguments
	args := contract.GetNetMapArgs{}

	// invoke smart contract call
	values, err := w.client.NetMap(args)
	if err != nil {
		return nil, errors.Wrap(err, "could not invoke smart contract")
	}

	// parse response and fill the network map
	nm := netmap.New()

	peerList := values.Peers()

	for i := range peerList {
		info := Info{}

		info.SetPublicKey(peerList[i].PublicKey())
		info.SetAddress(string(peerList[i].Address()))

		binOpts := peerList[i].Options()
		opts := make([]string, 0, len(binOpts))

		for j := range binOpts {
			opts = append(opts, string(binOpts[j]))
		}

		info.SetOptions(opts)

		if err := nm.AddNode(info); err != nil {
			return nil, errors.Wrapf(err, "could not add node #%d to network map", i)
		}
	}

	return nm, nil
}
