package locode_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/locode"
	locodestd "github.com/nspcc-dev/neofs-node/pkg/util/locode"
	locodedb "github.com/nspcc-dev/neofs-node/pkg/util/locode/db"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

type record struct {
	*locodedb.Key
	*locodedb.Record
}

type db struct {
	items map[locodestd.LOCODE]locode.Record
}

func (x db) add(lc locodestd.LOCODE, rec locode.Record) {
	x.items[lc] = rec
}

func (x db) Get(lc *locodestd.LOCODE) (locode.Record, error) {
	r, ok := x.items[*lc]
	if !ok {
		return nil, errors.New("record not found")
	}

	return r, nil
}

func addAttrKV(n *netmap.NodeInfo, key, val string) {
	a := netmap.NewNodeAttribute()

	a.SetKey(key)
	a.SetValue(val)

	n.SetAttributes(append(n.Attributes(), a)...)
}

func addLocodeAttrValue(n *netmap.NodeInfo, val string) {
	addAttrKV(n, netmap.AttrUNLOCODE, val)
}

func addLocodeAttr(n *netmap.NodeInfo, lc locodestd.LOCODE) {
	addLocodeAttrValue(n, fmt.Sprintf("%s %s", lc[0], lc[1]))
}

func nodeInfoWithSomeAttrs() *netmap.NodeInfo {
	n := netmap.NewNodeInfo()

	addAttrKV(n, "key1", "val1")
	addAttrKV(n, "key2", "val2")

	return n
}

func containsAttr(n *netmap.NodeInfo, key, val string) bool {
	for _, a := range n.Attributes() {
		if a.Key() == key && a.Value() == val {
			return true
		}
	}

	return false
}

func TestValidator_VerifyAndUpdate(t *testing.T) {
	db := &db{
		items: make(map[locodestd.LOCODE]locode.Record),
	}

	// test record with valid but random values
	r := locodestd.Record{
		LOCODE:           locodestd.LOCODE{"RU", "MOW"},
		NameWoDiacritics: "Moskva",
		SubDiv:           "MSK",
	}

	k, err := locodedb.NewKey(r.LOCODE)
	require.NoError(t, err)

	rdb, err := locodedb.NewRecord(r)
	require.NoError(t, err)

	rdb.SetCountryName("Russia")
	rdb.SetSubDivName("Moskva oblast")

	var cont locodedb.Continent = locodedb.ContinentEurope

	rdb.SetContinent(&cont)

	rec := record{
		Key:    k,
		Record: rdb,
	}

	db.add(r.LOCODE, rec)

	var p locode.Prm

	p.DB = db

	validator := locode.New(p)

	t.Run("w/ derived attributes", func(t *testing.T) {
		fn := func(withLocode bool) {
			for _, derivedAttr := range []string{
				netmap.AttrCountryCode,
				netmap.AttrCountry,
				netmap.AttrLocation,
				netmap.AttrSubDivCode,
				netmap.AttrSubDiv,
				netmap.AttrContinent,
			} {
				n := nodeInfoWithSomeAttrs()

				addAttrKV(n, derivedAttr, "some value")

				if withLocode {
					addLocodeAttr(n, r.LOCODE)
				}

				err := validator.VerifyAndUpdate(n)
				require.Error(t, err)
			}
		}

		fn(true)
		fn(false)
	})

	t.Run("w/o locode", func(t *testing.T) {
		n := nodeInfoWithSomeAttrs()

		attrs := n.Attributes()

		err := validator.VerifyAndUpdate(n)
		require.NoError(t, err)
		require.Equal(t, attrs, n.Attributes())
	})

	t.Run("w/ locode", func(t *testing.T) {
		t.Run("invalid locode", func(t *testing.T) {
			n := nodeInfoWithSomeAttrs()

			addLocodeAttrValue(n, "WRONG LOCODE")

			err := validator.VerifyAndUpdate(n)
			require.Error(t, err)
		})

		t.Run("missing DB record", func(t *testing.T) {
			n := nodeInfoWithSomeAttrs()

			addLocodeAttr(n, locodestd.LOCODE{"RU", "SPB"})

			err := validator.VerifyAndUpdate(n)
			require.Error(t, err)
		})

		n := nodeInfoWithSomeAttrs()

		addLocodeAttr(n, r.LOCODE)

		attrs := n.Attributes()

		err := validator.VerifyAndUpdate(n)
		require.NoError(t, err)

		outAttrs := n.Attributes()

		for _, a := range attrs {
			require.Contains(t, outAttrs, a)
		}

		require.True(t, containsAttr(n, netmap.AttrCountryCode, rec.CountryCode().String()))
		require.True(t, containsAttr(n, netmap.AttrCountry, rec.CountryName()))
		require.True(t, containsAttr(n, netmap.AttrLocation, rec.LocationName()))
		require.True(t, containsAttr(n, netmap.AttrSubDivCode, rec.SubDivCode()))
		require.True(t, containsAttr(n, netmap.AttrSubDiv, rec.SubDivName()))
		require.True(t, containsAttr(n, netmap.AttrContinent, rec.Continent().String()))
	})
}
