package locodedb

import (
	"testing"

	locodecolumn "github.com/nspcc-dev/neofs-node/pkg/util/locode/column"
	"github.com/stretchr/testify/require"
)

func TestPointFromCoordinates(t *testing.T) {
	testCases := []struct {
		latGot, longGot   string
		latWant, longWant float64
	}{
		{
			latGot:   "5915N",
			longGot:  "01806E",
			latWant:  59.25,
			longWant: 18.10,
		},
		{
			latGot:   "1000N",
			longGot:  "02030E",
			latWant:  10.00,
			longWant: 20.50,
		},
		{
			latGot:   "0145S",
			longGot:  "03512W",
			latWant:  -01.75,
			longWant: -35.20,
		},
	}

	var (
		crd   *locodecolumn.Coordinates
		point *Point
		err   error
	)

	for _, test := range testCases {
		crd, err = locodecolumn.CoordinatesFromString(test.latGot + " " + test.longGot)
		require.NoError(t, err)

		point, err = PointFromCoordinates(crd)
		require.NoError(t, err)

		require.Equal(t, test.latWant, point.Latitude())
		require.Equal(t, test.longWant, point.Longitude())
	}
}
