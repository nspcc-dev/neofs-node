package main

import (
	"strconv"

	sdk "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
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

		attrs = addWellKnownAttributes(c, attrs)

		peerInfo := new(v2netmap.NodeInfo)
		peerInfo.SetAddress(c.viper.GetString(cfgBootstrapAddress))
		peerInfo.SetPublicKey(crypto.MarshalPublicKey(&c.key.PublicKey))
		peerInfo.SetAttributes(attrs)

		err = cliWrapper.AddPeer(peerInfo)
		fatalOnErr(errors.Wrap(err, "bootstrap error"))
	}
}

func addWellKnownAttributes(c *cfg, attrs []*v2netmap.Attribute) []*v2netmap.Attribute {
	var hasCapacity, hasPrice bool

	// check if user defined capacity and price attributes
	for i := range attrs {
		if !hasPrice && attrs[i].GetKey() == sdk.PriceAttr {
			hasPrice = true
		} else if !hasCapacity && attrs[i].GetKey() == sdk.CapacityAttr {
			hasCapacity = true
		}
	}

	// do not override user defined capacity and price attributes

	if !hasCapacity {
		capacity := new(v2netmap.Attribute)
		capacity.SetKey(sdk.CapacityAttr)
		capacity.SetValue(strconv.FormatUint(c.cfgNodeInfo.capacity, 10))
		attrs = append(attrs, capacity)
	}

	if !hasPrice {
		price := new(v2netmap.Attribute)
		price.SetKey(sdk.PriceAttr)
		price.SetValue(strconv.FormatUint(c.cfgNodeInfo.price, 10))
		attrs = append(attrs, price)
	}

	return attrs
}
