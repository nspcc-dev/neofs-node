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

		words := strings.Split(line, keyValueSeparator)
		if len(words) != 2 {
			return errors.New("missing attribute key and/or value")
		}

		_, ok := cache[words[0]]
		if ok {
			return fmt.Errorf("duplicated keys %s", words[0])
		}

		cache[words[0]] = struct{}{}

		// replace non-printable symbols with escaped symbols without escape character
		words[0] = replaceEscaping(words[0], true)
		words[1] = replaceEscaping(words[1], true)

		if words[0] == "" {
			return errors.New("empty key")
		} else if words[1] == "" {
			return errors.New("empty value")
		}

		dst.SetAttribute(words[0], words[1])
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
