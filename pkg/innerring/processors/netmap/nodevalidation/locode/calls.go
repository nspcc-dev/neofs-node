package locode

import (
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// VerifyAndUpdate validates UN-LOCODE attribute of n
// and adds a group of related attributes.
//
// If n contains at least one of the LOCODE-derived attributes,
// an error is returned.
//
// If n contains UN-LOCODE attribute and its value does not
// match the UN/LOCODE format, an error is returned.
//
// UN-LOCODE attribute remains untouched.
func (v *Validator) VerifyAndUpdate(n *netmap.NodeInfo) error {
	if n.LOCODE() == "" {
		return nil
	}
	key, record, err := getRecord(n.LOCODE())
	if err != nil {
		return fmt.Errorf("could not get locode record from DB: %w", err)
	}

	n.SetCountryCode(key.CountryCode().String())
	n.SetCountryName(record.Country)
	n.SetLocationName(record.Location)
	n.SetContinentName(record.Cont.String())
	if subDivCode := record.SubDivCode; subDivCode != "" {
		n.SetSubdivisionCode(subDivCode)
	}

	if subDivName := record.SubDivName; subDivName != "" {
		n.SetSubdivisionName(subDivName)
	}

	return nil
}
