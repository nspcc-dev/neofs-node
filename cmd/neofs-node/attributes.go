package main

import (
	"fmt"

	"github.com/nspcc-dev/locode-db/pkg/locodedb"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
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

	key, record, err := getRecord(locAttr)
	if err != nil {
		fatalOnErr(fmt.Errorf("could not get locode record from DB: %w", err))
	}

	n := &c.cfgNodeInfo.localInfo

	setLocodeAttr(n, "CountryCode", key.CountryCode().String())
	setLocodeAttr(n, "Country", record.Country)
	setLocodeAttr(n, "Location", record.Location)
	setLocodeAttr(n, "Continent", record.Cont.String())
	if subDivCode := record.SubDivCode; subDivCode != "" {
		setLocodeAttr(n, "SubDivCode", subDivCode)
	}
	if subDivName := record.SubDivName; subDivName != "" {
		setLocodeAttr(n, "SubDiv", subDivName)
	}
}

func getRecord(lc string) (*locodedb.Key, locodedb.Record, error) {
	country, location := lc[:2], lc[2:]
	if lc[2] == ' ' {
		location = lc[3:]
	}
	key, err := locodedb.NewKey(country, location)
	if err != nil {
		return nil, locodedb.Record{}, err
	}
	rec, err := locodedb.Get(lc)
	return key, rec, err
}

func setLocodeAttr(ni *netmap.NodeInfo, key, value string) {
	valHave := ni.Attribute(key)
	if valHave != "" {
		return
	}

	ni.SetAttribute(key, value)
}
