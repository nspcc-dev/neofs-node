package common

import (
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// FormatAttributeValue returns formatted string representation of an object attribute value.
func FormatAttributeValue(key, value string, raw bool) string {
	if raw || key != object.AttributeTimestamp {
		return value
	}

	return fmt.Sprintf("%s (%s)", value, PrettyPrintUnixTime(value))
}
