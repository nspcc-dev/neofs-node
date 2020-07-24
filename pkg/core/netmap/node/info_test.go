package node

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInfo_Address(t *testing.T) {
	i := new(Info)

	addr := "address"
	i.SetAddress(addr)

	require.Equal(t, addr, i.Address())
}

func TestInfo_Status(t *testing.T) {
	i := new(Info)

	st := StatusFromUint64(1)
	i.SetStatus(st)

	require.Equal(t, st, i.Status())
}

func TestInfo_PublicKey(t *testing.T) {
	i := new(Info)

	key := []byte{1, 2, 3}
	i.SetPublicKey(key)

	require.Equal(t, key, i.PublicKey())
}

func TestCopyPublicKey(t *testing.T) {
	i := Info{}

	// set initial node key
	initKey := []byte{1, 2, 3}
	i.SetPublicKey(initKey)

	// get node key copy
	keyCopy := CopyPublicKey(i)

	// change the copy
	keyCopy[0]++

	// check that node key has not changed
	require.Equal(t, initKey, i.PublicKey())
}

func TestSetPublicKeyCopy(t *testing.T) {
	require.EqualError(t,
		SetPublicKeyCopy(nil, nil),
		ErrNilInfo.Error(),
	)

	i := new(Info)

	// create source key
	srcKey := []byte{1, 2, 3}

	// copy and set node key
	require.NoError(t, SetPublicKeyCopy(i, srcKey))

	// get node key
	nodeKey := i.PublicKey()

	// change the source key
	srcKey[0]++

	// check that node key has not changed
	require.Equal(t, nodeKey, i.PublicKey())
}

func TestInfo_Options(t *testing.T) {
	i := new(Info)

	opts := []string{
		"opt1",
		"opt2",
	}
	i.SetOptions(opts)

	require.Equal(t, opts, i.Options())
}

func TestCopyOptions(t *testing.T) {
	i := Info{}

	// set initial node options
	initOpts := []string{
		"opt1",
		"opt2",
	}
	i.SetOptions(initOpts)

	// get node options copy
	optsCopy := CopyOptions(i)

	// change the copy
	optsCopy[0] = "some other opt"

	// check that node options have not changed
	require.Equal(t, initOpts, i.Options())
}

func TestSetOptionsCopy(t *testing.T) {
	require.NotPanics(t, func() {
		SetOptionsCopy(nil, nil)
	})

	i := new(Info)

	// create source options
	srcOpts := []string{
		"opt1",
		"opt2",
	}

	// copy and set node options
	SetOptionsCopy(i, srcOpts)

	// get node options
	nodeOpts := i.Options()

	// change the source options
	srcOpts[0] = "some other opt"

	// check that node options have not changed
	require.Equal(t, nodeOpts, i.Options())
}
