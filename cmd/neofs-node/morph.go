package main

import (
	v2netmap "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/pkg/errors"
)

func initMorphComponents(c *cfg) {
	var err error

	c.cfgMorph.client, err = client.New(c.key, c.viper.GetString(cfgMorphRPCAddress))
	fatalOnErr(err)

	staticClient, err := client.NewStatic(
		c.cfgMorph.client,
		c.cfgNetmap.scriptHash,
		c.cfgContainer.fee,
	)
	fatalOnErr(err)

	cli, err := netmap.New(staticClient)
	fatalOnErr(err)

	wrap, err := wrapper.New(cli)
	fatalOnErr(err)

	c.cfgObject.netMapStorage = wrap
	c.cfgNetmap.wrapper = wrap
}

func bootstrapNode(c *cfg) {
	peerInfo := new(v2netmap.NodeInfo)
	peerInfo.SetAddress(c.localAddr.String())
	peerInfo.SetPublicKey(crypto.MarshalPublicKey(&c.key.PublicKey))
	peerInfo.SetAttributes(c.cfgNodeInfo.attributes)

	err := c.cfgNetmap.wrapper.AddPeer(peerInfo)
	fatalOnErr(errors.Wrap(err, "bootstrap error"))
}
