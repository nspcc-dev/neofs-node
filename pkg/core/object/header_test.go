package object

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func testHeaders(num uint32) []ExtendedHeader {
	res := make([]ExtendedHeader, num)

	for i := uint32(0); i < num; i++ {
		res[i].SetType(TypeFromUint32(i))
		res[i].SetValue(i)
	}

	return res
}

func TestObject_ExtendedHeaders(t *testing.T) {
	h := new(Header)

	hs := testHeaders(2)

	h.SetExtendedHeaders(hs)

	require.Equal(t, hs, h.ExtendedHeaders())
}

func TestCopyExtendedHeaders(t *testing.T) {
	require.Nil(t, CopyExtendedHeaders(nil))

	h := new(Header)

	// set initial headers
	initHs := testHeaders(2)
	h.SetExtendedHeaders(initHs)

	// get extended headers copy
	hsCopy := CopyExtendedHeaders(h)

	// change the copy
	hsCopy[0] = hsCopy[1]

	// check that extended headers have not changed
	require.Equal(t, initHs, h.ExtendedHeaders())
}

func TestSetExtendedHeadersCopy(t *testing.T) {
	require.NotPanics(t, func() {
		SetExtendedHeadersCopy(nil, nil)
	})

	h := new(Header)

	// create source headers
	srcHs := testHeaders(2)

	// copy and set headers
	SetExtendedHeadersCopy(h, srcHs)

	// get extended headers
	objHs := h.ExtendedHeaders()

	// change the source headers
	srcHs[0] = srcHs[1]

	// check that headeres have not changed
	require.Equal(t, objHs, h.ExtendedHeaders())
}

func TestHeaderRelations(t *testing.T) {
	items := []struct {
		relFn func(ExtendedHeaderType, ExtendedHeaderType) bool

		base, ok, fail uint32
	}{
		{relFn: TypesEQ, base: 1, ok: 1, fail: 2},
		{relFn: TypesLT, base: 1, ok: 2, fail: 0},
		{relFn: TypesGT, base: 1, ok: 0, fail: 2},
	}

	for _, item := range items {
		require.True(t,
			item.relFn(
				TypeFromUint32(item.base),
				TypeFromUint32(item.ok),
			),
		)

		require.False(t,
			item.relFn(
				TypeFromUint32(item.base),
				TypeFromUint32(item.fail),
			),
		)
	}
}
