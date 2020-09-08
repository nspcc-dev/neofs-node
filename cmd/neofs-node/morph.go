package main

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
)

func initMorphComponents(c *cfg) {
	var err error

	c.cfgMorph.client, err = client.New(c.key, c.cfgMorph.endpoint)
	fatalOnErr(err)
}

func bootstrapNode(c *cfg) {
	if c.cfgNodeInfo.bootType == StorageNode {
		u160, err := util.Uint160DecodeStringLE(c.cfgNetmap.scriptHash)
		fatalOnErr(err)

		staticClient, err := client.NewStatic(c.cfgMorph.client, u160, c.cfgContainer.fee)
		fatalOnErr(err)

		cli, err := netmap.New(staticClient)
		fatalOnErr(err)

		peerInfo := new(netmap.NodeInfo)
		peerInfo.SetAddress(c.cfgNodeInfo.address)
		peerInfo.SetPublicKey(crypto.MarshalPublicKey(&c.key.PublicKey))
		// todo: add attributes as opts

		rawInfo, err := peerInfo.StableMarshal(nil)
		fatalOnErr(err)

		args := new(netmap.AddPeerArgs)
		args.SetInfo(rawInfo)
		err = cli.AddPeer(*args)
		fatalOnErr(err)
	}
}
