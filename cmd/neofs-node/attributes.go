package main

import (
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
	"github.com/spf13/viper"
)

const (
	// list of default values for well-known attributes
	defaultCapacity = 0
	defaultPrice    = 0
)

func parseAttributes(v *viper.Viper) []*netmap.NodeAttribute {
	stringAttributes := readAttributes(v)

	attrs, err := attributes.ParseV2Attributes(stringAttributes, nil)
	if err != nil {
		fatalOnErr(err)
	}

	return addWellKnownAttributes(attrs)
}

func readAttributes(v *viper.Viper) (attrs []string) {
	const maxAttributes = 100

	for i := 0; i < maxAttributes; i++ {
		attr := v.GetString(cfgNodeAttributePrefix + "_" + strconv.Itoa(i))
		if attr == "" {
			return
		}

		attrs = append(attrs, attr)
	}

	return attrs
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
