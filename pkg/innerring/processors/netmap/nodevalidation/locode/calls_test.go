package locode_test

import (
	"fmt"
	"testing"

	"github.com/nspcc-dev/locode-db/pkg/locodedb"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/locode"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func addLocodeAttrValue(n *netmap.NodeInfo, val string) {
	n.SetLOCODE(val)
}

func addLocodeAttr(n *netmap.NodeInfo, lc [2]string) {
	n.SetLOCODE(fmt.Sprintf("%s %s", lc[0], lc[1]))
}

func nodeInfoWithSomeAttrs() *netmap.NodeInfo {
	var n netmap.NodeInfo

	n.SetAttribute("key1", "val1")
	n.SetAttribute("key2", "val2")

	return &n
}

func TestValidator_VerifyAndUpdate(t *testing.T) { // test record with valid but random values
	r := locodedb.Record{
		SubDivCode: "MSK",
	}

	k, err := locodedb.NewKey("RU", "MOW")
	require.NoError(t, err)
	r.Location = "Moskva"
	r.Country = "Russia"
	r.SubDivName = "Moskva"
	r.SubDivCode = "MOW"

	var cont locodedb.Continent = locodedb.ContinentEurope

	r.Cont = &cont

	validator := locode.New()

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

			addLocodeAttr(n, [2]string{"RU", "SPB"}) //RU,SSR -Saint Petersburg

			err := validator.VerifyAndUpdate(n)
			require.Error(t, err)
		})

		n := nodeInfoWithSomeAttrs()

		addLocodeAttr(n, [2]string{k.CountryCode().String(), k.LocationCode().String()})

		err := validator.VerifyAndUpdate(n)
		require.NoError(t, err)

		require.Equal(t, k.CountryCode().String(), n.Attribute("CountryCode"))
		require.Equal(t, r.Country, n.Attribute("Country"))
		require.Equal(t, r.Location, n.Attribute("Location"))
		require.Equal(t, r.SubDivCode, n.Attribute("SubDivCode"))
		require.Equal(t, r.SubDivName, n.Attribute("SubDiv"))
		require.Equal(t, r.Cont.String(), n.Attribute("Continent"))
	})
}
