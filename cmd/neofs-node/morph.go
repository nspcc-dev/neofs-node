package main

import (
	v2netmap "github.com/nspcc-dev/neofs-api-go/v2/netmap"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
	"github.com/pkg/errors"
)

func initMorphComponents(c *cfg) {
	var err error

	c.cfgMorph.client, err = client.New(c.key, c.viper.GetString(cfgMorphRPCAddress))
	fatalOnErr(err)
}

func bootstrapNode(c *cfg) {
	if c.cfgNodeInfo.bootType == StorageNode {
		staticClient, err := client.NewStatic(
			c.cfgMorph.client,
			c.cfgNetmap.scriptHash,
			c.cfgContainer.fee,
		)
		fatalOnErr(err)

		cli, err := netmap.New(staticClient)
		fatalOnErr(errors.Wrap(err, "bootstrap error"))

		cliWrapper, err := wrapper.New(cli)
		fatalOnErr(errors.Wrap(err, "bootstrap error"))

		attrs, err := attributes.ParseV2Attributes(c.cfgNodeInfo.attributes, nil)
		if err != nil {
			fatalOnErr(errors.Wrap(err, "bootstrap attribute error"))
		}

		peerInfo := new(v2netmap.NodeInfo)
		peerInfo.SetAddress(c.viper.GetString(cfgBootstrapAddress))
		peerInfo.SetPublicKey(crypto.MarshalPublicKey(&c.key.PublicKey))
		peerInfo.SetAttributes(attrs)

		err = cliWrapper.AddPeer(peerInfo)
		fatalOnErr(errors.Wrap(err, "bootstrap error"))
	}
}
