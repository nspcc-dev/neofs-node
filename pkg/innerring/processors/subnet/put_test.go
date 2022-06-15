package subnetevents

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/user"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"

	"github.com/nspcc-dev/neofs-sdk-go/subnet"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
)

type put struct {
	idEvent

	creator user.ID

	creatorErr error

	info subnet.Info

	infoErr error
}

func (x put) ReadCreator(id *user.ID) error {
	if x.creatorErr != nil {
		return x.creatorErr
	}

	*id = x.creator

	return nil
}

func (x put) ReadInfo(info *subnet.Info) error {
	if x.infoErr != nil {
		return x.infoErr
	}

	*info = x.info

	return nil
}

func TestPutValidator_Assert(t *testing.T) {
	var (
		v PutValidator

		e put

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
	e.id.SetNumeric(idNum)

	// read creator error
	e.creatorErr = errors.New("creator err")

	err = v.Assert(e)
	require.ErrorIs(t, err, e.creatorErr)

	e.creatorErr = nil

	// read info error
	e.infoErr = errors.New("info err")

	err = v.Assert(e)
	require.ErrorIs(t, err, e.infoErr)

	e.infoErr = nil

	// diff explicit ID and the one in info
	var id2 subnetid.ID

	id2.SetNumeric(idNum + 1)

	e.info.SetID(id2)

	err = v.Assert(e)
	require.ErrorIs(t, err, errDiffID)

	e.info.SetID(e.id)

	// diff explicit creator and the one in info
	creator2 := *usertest.ID()

	e.info.SetOwner(creator2)

	err = v.Assert(e)
	require.ErrorIs(t, err, errDiffOwner)

	e.info.SetOwner(e.creator)

	err = v.Assert(e)
	require.NoError(t, err)
}
