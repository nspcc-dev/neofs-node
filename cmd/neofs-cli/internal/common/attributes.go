package common

import (
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// FormatAttribute returns formatted string representation of an object attribute key-value pair.
func FormatAttribute(key, value string, raw bool) (string, string) {
	if raw {
		return strconv.Quote(key), strconv.Quote(value)
	}

	return key, formatAttributeValue(key, value)
}

func formatAttributeValue(key, value string) string {
	if key != object.AttributeTimestamp {
		return value
	}

	return fmt.Sprintf("%s (%s)", value, PrettyPrintUnixTime(value))
}
