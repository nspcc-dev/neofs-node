package main

import (
	"strconv"

	sdk "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/v2/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
	"github.com/spf13/viper"
)

const (
	// list of default values for well-known attributes
	defaultCapacity = 0
	defaultPrice    = 0
)

func parseAttributes(v *viper.Viper) []*netmap.Attribute {
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
		} else {
			attrs = append(attrs, attr)
		}
	}

	return attrs
}

func addWellKnownAttributes(attrs []*netmap.Attribute) []*netmap.Attribute {
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
		capacity := new(netmap.Attribute)
		capacity.SetKey(sdk.CapacityAttr)
		capacity.SetValue(strconv.FormatUint(defaultCapacity, 10))
		attrs = append(attrs, capacity)
	}

	if !hasPrice {
		price := new(netmap.Attribute)
		price.SetKey(sdk.PriceAttr)
		price.SetValue(strconv.FormatUint(defaultPrice, 10))
		attrs = append(attrs, price)
	}

	return attrs
}
