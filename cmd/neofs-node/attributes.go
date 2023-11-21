package main

import (
	"fmt"

	"github.com/nspcc-dev/locode-db/pkg/locodedb"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
)

func parseAttributes(c *cfg) {
	if nodeconfig.Relay(c.appCfg) {
		return
	}

	fatalOnErr(attributes.ReadNodeAttributes(&c.cfgNodeInfo.localInfo, nodeconfig.Attributes(c.appCfg)))

	// expand UN/LOCODE attribute if any found; keep user's attributes
	// if any conflicts appear

	locAttr := c.cfgNodeInfo.localInfo.LOCODE()
	if locAttr == "" {
		return
	}

	record, err := getRecord(locAttr)
	if err != nil {
		fatalOnErr(fmt.Errorf("could not get locode record from DB: %w", err))
	}

	countryCode := locAttr[:locodedb.CountryCodeLen]
	n := &c.cfgNodeInfo.localInfo

	if countryCode != "" && n.CountryCode() == "" {
		n.SetCountryCode(countryCode)
	}
	if record.Country != "" && n.CountryName() == "" {
		n.SetCountryName(record.Country)
	}
	if record.Location != "" && n.LocationName() == "" {
		n.SetLocationName(record.Location)
	}
	if record.Cont.String() != "" && n.ContinentName() == "" {
		n.SetContinentName(record.Cont.String())
	}
	if record.SubDivCode != "" && n.SubdivisionCode() == "" {
		n.SetSubdivisionCode(record.SubDivCode)
	}
	if record.SubDivName != "" && n.SubdivisionName() == "" {
		n.SetSubdivisionName(record.SubDivName)
	}
}

func getRecord(lc string) (locodedb.Record, error) {
	rec, err := locodedb.Get(lc)
	return rec, err
}
