package attributes

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

const keyValueSeparator = ":"

// ReadNodeAttributes parses node attributes from list of string in "Key:Value" format
// and writes them into netmap.NodeInfo instance. Supports escaped symbols
// "\:", "\/" and "\\".
func ReadNodeAttributes(dst *netmap.NodeInfo, attrs []string) error {
	cache := make(map[string]struct{}, len(attrs))

	for i := range attrs {
		line := replaceEscaping(attrs[i], false) // replaced escaped symbols with non-printable symbols

		key, value, found := strings.Cut(line, keyValueSeparator)
		if !found {
			return errors.New("missing attribute key and/or value")
		}

		_, ok := cache[key]
		if ok {
			return fmt.Errorf("duplicated keys %s", key)
		}

		cache[key] = struct{}{}

		// replace non-printable symbols with escaped symbols without escape character
		key = replaceEscaping(key, true)
		value = replaceEscaping(value, true)

		if key == "" {
			return errors.New("empty key")
		} else if value == "" {
			return errors.New("empty value")
		}

		dst.SetAttribute(key, value)
	}

	return nil
}

func replaceEscaping(target string, rollback bool) (s string) {
	const escChar = `\`

	var (
		oldKVSep = escChar + keyValueSeparator
		oldEsc   = escChar + escChar
		newKVSep = string(uint8(2))
		newEsc   = string(uint8(3))
	)

	if rollback {
		oldKVSep, oldEsc = newKVSep, newEsc
		newKVSep = keyValueSeparator
		newEsc = escChar
	}

	s = strings.ReplaceAll(target, oldEsc, newEsc)
	s = strings.ReplaceAll(s, oldKVSep, newKVSep)

	return
}
