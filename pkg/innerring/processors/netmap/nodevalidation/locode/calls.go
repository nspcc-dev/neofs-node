package locode

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/util/locode"
	"github.com/pkg/errors"
)

var errMissingLocode = errors.New("missing locode attribute")

var errMissingRequiredAttr = errors.New("missing required attribute in DB record")

// VerifyAndUpdate validates UN-LOCODE attribute of n
// and adds a group of related attributes.
//
// If n does not contain UN-LOCODE attribute or its value does not
// match the UN/LOCODE format, an error returns.
//
// New attributes are formed from the record of DB instance (Prm).
// If DB entry R was found w/o errors, then new attributes are:
//  * CountryCode: R.CountryCode().String();
//  * Country: R.CountryName();
//  * LocationCode: R.LocationCode().String();
//  * Location: Record.LocationName();
//  * SubDivCode: R.SubDivCode();
//  * SubDiv: R.SubDivName();
//  * Continent: R.Continent().String().
//
// UN-LOCODE attribute remains untouched.
func (v *Validator) VerifyAndUpdate(n *netmap.NodeInfo) error {
	mAttr := uniqueAttributes(n.Attributes())

	attrLocode, ok := mAttr[netmap.AttrUNLOCODE]
	if !ok {
		return errMissingLocode
	}

	lc, err := locode.FromString(attrLocode.Value())
	if err != nil {
		return errors.Wrap(err, "invalid locode value")
	}

	record, err := v.db.Get(lc)
	if err != nil {
		return errors.Wrap(err, "could not get locode record from DB")
	}

	for attrKey, attrDesc := range v.mAttr {
		attrVal := attrDesc.converter(record)
		if attrVal == "" {
			if !attrDesc.optional {
				return errMissingRequiredAttr
			}

			continue
		}

		a := netmap.NewNodeAttribute()
		a.SetKey(attrKey)
		a.SetValue(attrVal)

		mAttr[attrKey] = a
	}

	as := n.Attributes()
	as = as[:0]

	for _, attr := range mAttr {
		as = append(as, attr)
	}

	n.SetAttributes(as...)

	return nil
}

func uniqueAttributes(as []*netmap.NodeAttribute) map[string]*netmap.NodeAttribute {
	mAttr := make(map[string]*netmap.NodeAttribute, len(as))

	for _, attr := range as {
		mAttr[attr.Key()] = attr
	}

	return mAttr
}
