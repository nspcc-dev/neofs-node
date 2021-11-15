package engine

import (
	"errors"
	"testing"

	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestExecBlocks(t *testing.T) {
	e := testNewEngineWithShardNum(t, 2) // number doesn't matter in this test, 2 is several but not many
	defer e.Close()

	// put some object
	obj := generateRawObjectWithCID(t, cidtest.GenerateID()).Object()

	addr := obj.Address()

	require.NoError(t, Put(e, obj))

	// block executions
	errBlock := errors.New("block exec err")

	require.NoError(t, e.BlockExecution(errBlock))

	// try to exec some op
	_, err := Head(e, addr)
	require.ErrorIs(t, err, errBlock)

	// resume executions
	require.NoError(t, e.ResumeExecution())

	_, err = Head(e, addr) // can be any data-related op
	require.NoError(t, err)

	// close
	require.NoError(t, e.Close())

	// try exec after close
	_, err = Head(e, addr)
	require.Error(t, err)

	// try to resume
	require.Error(t, e.ResumeExecution())
}
