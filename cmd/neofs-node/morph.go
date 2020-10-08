package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap/wrapper"
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
