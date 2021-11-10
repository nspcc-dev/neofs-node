package main

import (
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

const (
	// list of default values for well-known attributes
	defaultCapacity = 0
	defaultPrice    = 0
)

func parseAttributes(c *config.Config) []*netmap.NodeAttribute {
	stringAttributes := nodeconfig.Attributes(c)

	attrs, err := attributes.ParseV2Attributes(stringAttributes, nil)
	if err != nil {
		fatalOnErr(err)
	}

	return addWellKnownAttributes(attrs)
}

type wellKnownNodeAttrDesc struct {
	explicit   bool
	defaultVal string
}

func listWellKnownAttrDesc() map[string]wellKnownNodeAttrDesc {
	return map[string]wellKnownNodeAttrDesc{
		netmap.AttrPrice:    {defaultVal: strconv.FormatUint(defaultPrice, 10)},
		netmap.AttrCapacity: {defaultVal: strconv.FormatUint(defaultCapacity, 10)},
		netmap.AttrUNLOCODE: {explicit: true},
	}
}

func addWellKnownAttributes(attrs []*netmap.NodeAttribute) []*netmap.NodeAttribute {
	mWellKnown := listWellKnownAttrDesc()

	// check how user defined well-known attributes
	for i := range attrs {
		delete(mWellKnown, attrs[i].Key())
	}

	for key, desc := range mWellKnown {
		// check if required attribute is set
		if desc.explicit {
			fatalOnErr(fmt.Errorf("missing explicit value of required node attribute %s", key))
		}

		// set default value of the attribute
		a := netmap.NewNodeAttribute()
		a.SetKey(key)
		a.SetValue(desc.defaultVal)

		attrs = append(attrs, a)
	}

	return attrs
}
