package locode

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

var errMissingRequiredAttr = errors.New("missing required attribute in DB record")

// VerifyAndUpdate validates UN-LOCODE attribute of n
// and adds a group of related attributes.
//
// If n contains at least one of the LOCODE-derived attributes,
// an error is returned.
//
// If n contains UN-LOCODE attribute and its value does not
// match the UN/LOCODE format, an error is returned.
//
// New attributes are formed from the record of DB instance (Prm).
// If DB entry R was found w/o errors, new attributes are:
//   - CountryCode: R.CountryCode().String();
//   - Country: R.CountryName();
//   - Location: Record.LocationName();
//   - SubDivCode: R.SubDivCode();
//   - SubDiv: R.SubDivName();
//   - Continent: R.Continent().String().
//
// UN-LOCODE attribute remains untouched.
func (v *Validator) VerifyAndUpdate(n *netmap.NodeInfo) error {
	attrLocode := n.LOCODE()
	if attrLocode == "" {
		return nil
	}

	lc, err := locode.FromString(attrLocode)
	if err != nil {
		return fmt.Errorf("invalid locode value: %w", err)
	}

	record, err := v.db.Get(lc)
	if err != nil {
		return fmt.Errorf("could not get locode record from DB: %w", err)
	}

	countryCode := record.CountryCode()
	if countryCode == nil {
		return errMissingRequiredAttr
	}

	strCountryCode := countryCode.String()
	if strCountryCode == "" {
		return errMissingRequiredAttr
	}

	countryName := record.CountryName()
	if countryName == "" {
		return errMissingRequiredAttr
	}

	locationName := record.LocationName()
	if locationName == "" {
		return errMissingRequiredAttr
	}

	continent := record.Continent()
	if continent == nil {
		return errMissingRequiredAttr
	}

	continentName := continent.String()
	if continentName == "" {
		return errMissingRequiredAttr
	}

	n.SetCountryCode(strCountryCode)
	n.SetCountryName(countryName)
	n.SetLocationName(locationName)
	n.SetContinentName(continentName)

	if subDivCode := record.SubDivCode(); subDivCode != "" {
		n.SetSubdivisionCode(subDivCode)
	}

	if subDivName := record.SubDivName(); subDivName != "" {
		n.SetSubdivisionName(subDivName)
	}

	return nil
}
