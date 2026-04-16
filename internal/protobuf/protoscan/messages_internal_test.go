package protoscan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSimpleFieldsScheme(t *testing.T) {
	require.PanicsWithValue(t, "duplicated field with number 42", func() {
		newSimpleFieldsScheme(
			newSimpleField(42, "foo", FieldTypeBytes),
			newSimpleField(42, "bar", FieldTypeString),
		)
	})
}
