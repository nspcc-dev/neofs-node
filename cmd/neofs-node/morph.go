package main

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

func initMorphComponents(c *cfg) {
	var err error

	c.morphClient, err = client.New(c.key, c.morphEndpoint)

	fatalOnErr(err)
}
