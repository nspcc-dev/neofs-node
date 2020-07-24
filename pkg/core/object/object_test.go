package object

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObject_Payload(t *testing.T) {
	o := new(Object)

	payload := []byte{1, 2, 3}
	o.SetPayload(payload)

	require.Equal(t, payload, o.Payload())
}

func TestCopyPayload(t *testing.T) {
	require.Nil(t, CopyPayload(nil))

	o := new(Object)

	// set initial node key
	initPayload := []byte{1, 2, 3}
	o.SetPayload(initPayload)

	// get payload copy
	pCopy := CopyPayload(o)

	// change the copy
	pCopy[0]++

	// check that payload has not changed
	require.Equal(t, initPayload, o.Payload())
}

func TestSetPayloadCopy(t *testing.T) {
	require.NotPanics(t, func() {
		SetExtendedHeadersCopy(nil, nil)
	})

	o := new(Object)

	// create source payload
	srcPayload := []byte{1, 2, 3}

	// copy and set payload
	SetPayloadCopy(o, srcPayload)

	// get payload
	objPayload := o.Payload()

	// change the source payload
	srcPayload[0]++

	// check that payload has not changed
	require.Equal(t, objPayload, o.Payload())
}
