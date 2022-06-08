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

func addLocodeAttrValue(n *netmap.NodeInfo, val string) {
	n.SetLOCODE(val)
}

func addLocodeAttr(n *netmap.NodeInfo, lc locodestd.LOCODE) {
	n.SetLOCODE(fmt.Sprintf("%s %s", lc[0], lc[1]))
}

func nodeInfoWithSomeAttrs() *netmap.NodeInfo {
	var n netmap.NodeInfo

	n.SetAttribute("key1", "val1")
	n.SetAttribute("key2", "val2")

	return &n
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

	t.Run("w/o locode", func(t *testing.T) {
		n := nodeInfoWithSomeAttrs()

		err := validator.VerifyAndUpdate(n)
		require.NoError(t, err)
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

		err := validator.VerifyAndUpdate(n)
		require.NoError(t, err)

		require.Equal(t, rec.CountryCode().String(), n.Attribute("CountryCode"))
		require.Equal(t, rec.CountryName(), n.Attribute("Country"))
		require.Equal(t, rec.LocationName(), n.Attribute("Location"))
		require.Equal(t, rec.SubDivCode(), n.Attribute("SubDivCode"))
		require.Equal(t, rec.SubDivName(), n.Attribute("SubDiv"))
		require.Equal(t, rec.Continent().String(), n.Attribute("Continent"))
	})
}
