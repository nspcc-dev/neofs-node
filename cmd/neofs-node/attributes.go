package main

import (
	"fmt"

	"github.com/nspcc-dev/locode-db/pkg/locodedb"
	nodeconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/node"
	"github.com/nspcc-dev/neofs-node/pkg/util/attributes"
	"go.uber.org/zap"
)

func parseAttributes(c *cfg) {
	if nodeconfig.Relay(c.cfgReader) {
		return
	}

	fatalOnErr(attributes.ReadNodeAttributes(&c.cfgNodeInfo.localInfo, nodeconfig.Attributes(c.cfgReader)))

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

	fromUser := n.CountryCode()
	if fromUser != "" && fromUser != countryCode {
		c.log.Warn("locode attribute mismatch, configuration value kept: country code",
			zap.String("configuration_value", fromUser),
			zap.String("database_value", countryCode))
	} else {
		setIfNotEmpty(n.SetCountryCode, countryCode)
	}

	fromUser = n.CountryName()
	if fromUser != "" && fromUser != record.Country {
		c.log.Warn("locode attribute mismatch, configuration value kept: country name",
			zap.String("configuration_value", fromUser),
			zap.String("database_value", record.Country))
	} else {
		setIfNotEmpty(n.SetCountryName, record.Country)
	}

	fromUser = n.LocationName()
	if fromUser != "" && fromUser != record.Location {
		c.log.Warn("locode attribute mismatch, configuration value kept: location name",
			zap.String("configuration_value", fromUser),
			zap.String("database_value", record.Location))
	} else {
		setIfNotEmpty(n.SetLocationName, record.Location)
	}

	fromUser = n.ContinentName()
	if fromUser != "" && fromUser != record.Cont.String() {
		c.log.Warn("locode attribute mismatch, configuration value kept: continent",
			zap.String("configuration_value", fromUser),
			zap.String("database_value", record.Cont.String()))
	} else {
		setIfNotEmpty(n.SetContinentName, record.Cont.String())
	}

	fromUser = n.SubdivisionCode()
	if fromUser != "" && fromUser != record.SubDivCode {
		c.log.Warn("locode attribute mismatch, configuration value kept: subdivision code",
			zap.String("configuration_value", fromUser),
			zap.String("database_value", record.SubDivCode))
	} else {
		setIfNotEmpty(n.SetSubdivisionCode, record.SubDivCode)
	}

	fromUser = n.SubdivisionName()
	if fromUser != "" && fromUser != record.SubDivName {
		c.log.Warn("locode attribute mismatch, configuration value kept: subdivision name",
			zap.String("configuration_value", fromUser),
			zap.String("database_value", record.SubDivName))
	} else {
		setIfNotEmpty(n.SetSubdivisionName, record.SubDivName)
	}
}

func getRecord(lc string) (locodedb.Record, error) {
	rec, err := locodedb.Get(lc)
	return rec, err
}

func setIfNotEmpty(setter func(string), value string) {
	if value != "" {
		setter(value)
	}
}

func nodeAttrsEqual(arr1, arr2 [][2]string) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	elements := make(map[string]string, len(arr1))

	for _, item := range arr1 {
		elements[item[0]] = item[1]
	}

	for _, item := range arr2 {
		if value, exists := elements[item[0]]; !exists || value != item[1] {
			return false
		}
	}

	return true
}
