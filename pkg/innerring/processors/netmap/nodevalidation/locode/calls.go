package locode

import (
	"fmt"

	"github.com/nspcc-dev/locode-db/pkg/locodedb"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// Verify validates UN-LOCODE attribute of n
// and adds a group of related attributes.
//
// If n contains at least one of the LOCODE-derived attributes,
// an error is returned.
//
// If n contains UN-LOCODE attribute and its value does not
// match the UN/LOCODE format, an error is returned.
func (v *Validator) Verify(n netmap.NodeInfo) error {
	lAttr := n.LOCODE()
	if lAttr == "" {
		return nil
	}
	record, err := getRecord(lAttr)
	if err != nil {
		return fmt.Errorf("could not get locode record from DB: %w", err)
	}

	err = checkAttribute(n, "CountryCode", lAttr[:locodedb.CountryCodeLen])
	if err != nil {
		return err
	}
	err = checkAttribute(n, "Country", record.Country)
	if err != nil {
		return err
	}
	err = checkAttribute(n, "Location", record.Location)
	if err != nil {
		return err
	}
	err = checkAttribute(n, "Continent", record.Cont.String())
	if err != nil {
		return err
	}
	err = checkAttribute(n, "SubDivCode", record.SubDivCode)
	if err != nil {
		return err
	}
	err = checkAttribute(n, "SubDiv", record.SubDivName)
	if err != nil {
		return err
	}

	return nil
}

func checkAttribute(n netmap.NodeInfo, key, expectedVal string) error {
	val := n.Attribute(key)
	if val != expectedVal {
		return fmt.Errorf("wrong '%q' attribute value: want '%q', got '%q'", key, expectedVal, val)
	}

	return nil
}
