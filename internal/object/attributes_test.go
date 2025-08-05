package object_test

import (
	"strconv"
	"testing"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestGetIndexAttribute(t *testing.T) {
	var obj object.Object
	const attr = "attr"

	t.Run("missing", func(t *testing.T) {
		i, err := iobject.GetIndexAttribute(obj, attr)
		require.NoError(t, err)
		require.EqualValues(t, -1, i)
	})

	t.Run("not an integer", func(t *testing.T) {
		obj.SetAttributes(object.NewAttribute(attr, "not_an_int"))

		_, err := iobject.GetIndexAttribute(obj, attr)
		require.ErrorContains(t, err, "invalid syntax")
	})

	t.Run("negative", func(t *testing.T) {
		obj.SetAttributes(object.NewAttribute(attr, "-123"))

		_, err := iobject.GetIndexAttribute(obj, attr)
		require.EqualError(t, err, "negative value -123")
	})

	obj.SetAttributes(object.NewAttribute(attr, "1234567890"))

	i, err := iobject.GetIndexAttribute(obj, attr)
	require.NoError(t, err)
	require.EqualValues(t, 1234567890, i)

	t.Run("multiple", func(t *testing.T) {
		for _, s := range []string{
			"not_an_int",
			"-1",
			"2",
		} {
			obj.SetAttributes(
				object.NewAttribute(attr, "1"),
				object.NewAttribute(attr, s),
			)

			i, err := iobject.GetIndexAttribute(obj, attr)
			require.NoError(t, err)
			require.EqualValues(t, 1, i)
		}
	})
}

func TestGetIntAttribute(t *testing.T) {
	var obj object.Object
	const attr = "attr"

	t.Run("missing", func(t *testing.T) {
		_, err := iobject.GetIntAttribute(obj, attr)
		require.ErrorIs(t, err, iobject.ErrAttributeNotFound)
	})

	t.Run("not an integer", func(t *testing.T) {
		obj.SetAttributes(object.NewAttribute(attr, "not_an_int"))

		_, err := iobject.GetIntAttribute(obj, attr)
		require.ErrorContains(t, err, "invalid syntax")
	})

	for _, tc := range []struct {
		s string
		i int
	}{
		{s: "1234567890", i: 1234567890},
		{s: "0", i: 0},
		{s: "-1234567890", i: -1234567890},
	} {
		obj.SetAttributes(object.NewAttribute(attr, tc.s))

		i, err := iobject.GetIntAttribute(obj, attr)
		require.NoError(t, err, tc.s)
		require.EqualValues(t, tc.i, i)
	}

	t.Run("multiple", func(t *testing.T) {
		for _, s := range []string{
			"not_an_int",
			"-1",
			"2",
		} {
			obj.SetAttributes(
				object.NewAttribute(attr, "1"),
				object.NewAttribute(attr, s),
			)

			i, err := iobject.GetIntAttribute(obj, attr)
			require.NoError(t, err)
			require.EqualValues(t, 1, i)
		}
	})
}

func TestGetAttribute(t *testing.T) {
	var obj object.Object
	const attr = "attr"

	t.Run("missing", func(t *testing.T) {
		require.Empty(t, iobject.GetAttribute(obj, attr))
	})

	obj.SetAttributes(object.NewAttribute(attr, "val"))
	require.Equal(t, "val", iobject.GetAttribute(obj, attr))

	t.Run("multiple", func(t *testing.T) {
		obj.SetAttributes(
			object.NewAttribute(attr, "val1"),
			object.NewAttribute(attr, "val2"),
		)

		require.Equal(t, "val1", iobject.GetAttribute(obj, attr))
	})
}

func TestSetIntAttribute(t *testing.T) {
	var obj object.Object
	const attr = "attr"

	obj.SetAttributes(object.NewAttribute(attr+"_other", "val"))

	check := func(t *testing.T, val int) {
		iobject.SetIntAttribute(&obj, attr, val)

		attrs := obj.Attributes()
		require.Len(t, attrs, 2)
		require.Equal(t, attr, attrs[1].Key())
		require.Equal(t, strconv.Itoa(val), attrs[1].Value())

		got, err := iobject.GetIntAttribute(obj, attr)
		require.NoError(t, err, val)
		require.EqualValues(t, val, got)
	}

	check(t, 1234567890)
	check(t, 0)
	check(t, -1234567890)
}

func TestSetAttribute(t *testing.T) {
	var obj object.Object
	const attr = "attr"

	obj.SetAttributes(object.NewAttribute(attr+"_other", "val"))

	check := func(t *testing.T, val string) {
		iobject.SetAttribute(&obj, attr, val)

		attrs := obj.Attributes()
		require.Len(t, attrs, 2)
		require.Equal(t, attr, attrs[1].Key())
		require.Equal(t, val, attrs[1].Value())

		got := iobject.GetAttribute(obj, attr)
		require.Equal(t, val, got)
	}

	check(t, "val1")
	check(t, "val2")
}
