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

func nodeInfoWithSomeAttrs() netmap.NodeInfo {
	var n netmap.NodeInfo

	n.SetAttribute("key1", "val1")
	n.SetAttribute("key2", "val2")

	return n
}

func TestValidator_Verify(t *testing.T) { // test record with valid but random values
	r := locodedb.Record{
		SubDivCode: "MSK",
	}

	r.Location = "Moskva"
	r.Country = "Russia"
	r.SubDivName = "Moskva"
	r.SubDivCode = "MOW"

	var cont locodedb.Continent = locodedb.ContinentEurope

	r.Cont = cont

	validator := locode.New()

	t.Run("w/o locode", func(t *testing.T) {
		n := nodeInfoWithSomeAttrs()

		err := validator.Verify(n)
		require.NoError(t, err)
	})

	t.Run("w/ locode", func(t *testing.T) {
		t.Run("invalid locode", func(t *testing.T) {
			n := nodeInfoWithSomeAttrs()
			addLocodeAttrValue(&n, "WRONG LOCODE")

			err := validator.Verify(n)
			require.Error(t, err)
		})

		t.Run("missing DB record", func(t *testing.T) {
			n := nodeInfoWithSomeAttrs()

			addLocodeAttr(&n, [2]string{"RU", "SPB"}) // RU,SSR -Saint Petersburg

			err := validator.Verify(n)
			require.Error(t, err)
		})

		t.Run("correct code", func(t *testing.T) {
			n := nodeInfoWithSomeAttrs()
			addLocodeAttr(&n, [2]string{"RU", "MOW"})

			n.SetAttribute("CountryCode", "RU")
			n.SetAttribute("Country", r.Country)
			n.SetAttribute("Location", r.Location)
			n.SetAttribute("SubDivCode", r.SubDivCode)
			n.SetAttribute("SubDiv", r.SubDivName)
			n.SetAttribute("Continent", r.Cont.String())

			require.NoError(t, validator.Verify(n))
		})
		t.Run("invalid SN expansion", func(t *testing.T) {
			t.Run("invalid Country", func(t *testing.T) {
				n := nodeInfoWithSomeAttrs()
				addLocodeAttr(&n, [2]string{"RU", "SPB"})
				n.SetAttribute("CountryCode", r.Country+"bad")

				require.Error(t, validator.Verify(n))
			})

			t.Run("invalid Location", func(t *testing.T) {
				n := nodeInfoWithSomeAttrs()
				addLocodeAttr(&n, [2]string{"RU", "SPB"})
				n.SetAttribute("Location", r.Location+"bad")

				require.Error(t, validator.Verify(n))
			})

			t.Run("invalid SubDivCode", func(t *testing.T) {
				n := nodeInfoWithSomeAttrs()
				addLocodeAttr(&n, [2]string{"RU", "SPB"})
				n.SetAttribute("SubDivCode", r.SubDivCode+"bad")

				require.Error(t, validator.Verify(n))
			})

			t.Run("invalid SubDivName", func(t *testing.T) {
				n := nodeInfoWithSomeAttrs()
				addLocodeAttr(&n, [2]string{"RU", "SPB"})
				n.SetAttribute("SubDivName", r.SubDivName+"bad")

				require.Error(t, validator.Verify(n))
			})

			t.Run("invalid Continent", func(t *testing.T) {
				n := nodeInfoWithSomeAttrs()
				addLocodeAttr(&n, [2]string{"RU", "SPB"})
				n.SetAttribute("Continent", r.Cont.String()+"bad")

				require.Error(t, validator.Verify(n))
			})
		})
	})
}
