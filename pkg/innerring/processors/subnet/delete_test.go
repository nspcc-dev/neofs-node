package subnetevents

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
)

type delete struct {
	idEvent
}

func TestDeleteValidator_Assert(t *testing.T) {
	var (
		v DeleteValidator

		e delete

		err error
	)

	// read ID error
	e.idErr = errors.New("id err")

	err = v.Assert(e)
	require.ErrorIs(t, err, e.idErr)

	e.idErr = nil

	// zero subnet ID
	subnetid.MakeZero(&e.id)

	err = v.Assert(e)
	require.ErrorAs(t, err, new(zeroSubnetOp))

	const idNum = 13
	e.id.SetNumber(idNum)

	err = v.Assert(e)
	require.NoError(t, err)
}
