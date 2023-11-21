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

	if want, got := lAttr[:locodedb.CountryCodeLen], n.CountryCode(); want != got {
		return wrongLocodeAttrErr("country code", want, got)
	}
	if want, got := record.Country, n.CountryName(); want != got {
		return wrongLocodeAttrErr("country name", want, got)
	}
	if want, got := record.Location, n.LocationName(); want != got {
		return wrongLocodeAttrErr("location", want, got)
	}
	if want, got := record.Cont.String(), n.ContinentName(); want != got {
		return wrongLocodeAttrErr("continent", want, got)
	}
	if want, got := record.SubDivCode, n.SubdivisionCode(); want != got {
		return wrongLocodeAttrErr("subdivision code", want, got)
	}
	if want, got := record.SubDivName, n.SubdivisionName(); want != got {
		return wrongLocodeAttrErr("subdivision name", want, got)
	}

	return nil
}

func wrongLocodeAttrErr(attrName, want, got string) error {
	return fmt.Errorf("wrong %q attribute value: want '%q', got '%q'", attrName, want, got)
}
