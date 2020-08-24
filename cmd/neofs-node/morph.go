package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

func initMorphComponents(c *cfg) {
	var err error

	c.cfgMorph.client, err = client.New(c.key, c.cfgMorph.endpoint)

	fatalOnErr(err)
}
